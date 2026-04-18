"""
load_postgres.py
================
Phase 3 — ETL Pipeline: Idempotent Star Schema Load to PostgreSQL

Reads the validated flat merged Parquet and loads it into the ``icetex_ods_dw``
PostgreSQL star schema using two distinct idempotency strategies:

DIMENSION STRATEGY — ``INSERT … ON CONFLICT DO NOTHING``
---------------------------------------------------------
Each dimension table is keyed on its natural business key (e.g. ``vigencia +
periodo_otorgamiento`` for ``dim_period``).  On every pipeline run we attempt
to insert all unique combinations found in the source data.  Rows that already
exist in the DB are silently skipped; new rows are inserted and receive a fresh
SERIAL surrogate key.  After the insert phase, a ``SELECT`` retrieves the
authoritative DB-generated SKs for ALL natural keys (new and existing), which
are then merged back onto the fact DataFrame.

Why not ``INSERT … ON CONFLICT DO UPDATE``?  Dimension attributes (e.g. a
department name correction) should go through a proper SCD process with
versioning, not a silent overwrite.  ``DO NOTHING`` makes this an explicit
decision — a future SCD-2 upgrade can swap in ``DO UPDATE SET ... = EXCLUDED.``
per column as needed.

FACT TABLE STRATEGY — Watermark DELETE + chunked INSERT
---------------------------------------------------------
``fact_credits`` has no single-column natural key suitable for ``ON CONFLICT``;
its grain is the full combination of four SK foreign keys plus ``rango_valor``.
Adding a UNIQUE constraint over all five columns would create a wide index that
degrades write performance on a 100k+ row table loaded monthly.

Instead, we use a **year-watermark** approach:
  1. Identify all ``vigencia`` years present in the current Parquet.
  2. ``DELETE FROM fact_credits WHERE sk_period IN (SELECT sk_period FROM
     dim_period WHERE vigencia = ANY(:years))``.  This removes only the cohorts
     being re-loaded, leaving historical years untouched.
  3. ``INSERT`` the new fact rows in chunks of ``CHUNK_SIZE``.

Trade-off: there is a brief window between DELETE and INSERT where fact rows
for the affected years are absent.  This is acceptable in a monthly batch
context.  The entire operation runs inside a single ``engine.begin()``
transaction, so a mid-load failure rolls back to the pre-load state.

Airflow integration
-------------------
The function signature follows the XCom path-passing pattern::

    @task
    def load_data(validated_parquet: str) -> None:
        load_to_datawarehouse(
            parquet_path=validated_parquet,
            db_uri=os.environ["ICETEX_DW_URI"],
        )
"""

from __future__ import annotations

import logging
import traceback
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

# ─── Logger ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-26s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Tuning ───────────────────────────────────────────────────────────────────
CHUNK_SIZE: int = 5_000   # rows per INSERT batch — balances memory and round-trips

# ─── Source → DB column mappings ─────────────────────────────────────────────
# Keys   = column names in the merged Parquet (ICETEX original, uppercase)
# Values = (db_column_name, pandas_dtype_to_cast)
#
# "CÓDIGO DEDEPARTAMENTO DE ORIGEN" preserves the typo present in the ICETEX
# CSV source.  It maps to the integer DIVIPOLA code in the DW schema.

_PERIOD_MAP: dict[str, tuple[str, str]] = {
    "VIGENCIA":               ("vigencia",             "int32"),
    "PERIODO OTORGAMIENTO":   ("periodo_otorgamiento", "str"),
}

_GEO_MAP: dict[str, tuple[str, str]] = {
    "CÓDIGO DEDEPARTAMENTO DE ORIGEN": ("codigo_departamento",  "int32"),
    "DEPARTAMENTO DE ORIGEN":          ("departamento",          "str"),
    "CATEGORÍA DEL MUNICIPIO DE ORIGEN": ("categoria_municipio", "str"),
}

_PROGRAM_MAP: dict[str, tuple[str, str]] = {
    "SECTOR IES":          ("sector_ies",        "str"),
    "NIVEL DE FORMACIÓN":  ("nivel_formacion",   "str"),
    "MODALIDAD DE LÍNEA":  ("modalidad_linea",   "str"),
    "MODALIDAD DEL CRÉDITO": ("modalidad_credito", "str"),
}

_STUDENT_MAP: dict[str, tuple[str, str]] = {
    "SEXO AL NACER":          ("sexo_al_nacer",         "str"),
    "ESTRATO SOCIOECONÓMICO": ("estrato_socioeconomico", "int32"),
}

_FACT_MEASURE_MAP: dict[str, tuple[str, str]] = {
    "RANGO DEL VALOR TOTAL DESEMBOLSADO":           ("rango_valor_desembolsado",   "str"),
    "NÚMERO DE NUEVOS BENEFICIARIOS DE CRÉDITO":    ("total_nuevos_beneficiarios", "int32"),
}

# ─── Macroeconomic column mapping (Parquet → DB) ─────────────────────────────
# Keys   = column names as they appear in the merged Parquet produced by
#          transform_and_merge.py (lowercase, from the Socrata API).
# Values = (db_column_name, pandas_dtype_to_cast)
#
# These columns are NULLABLE in fact_credits.  If a Parquet does not contain a
# given column (e.g. the pipeline ran against IPC only, not Pobreza), the
# missing column is filled with NaN (→ NULL in PostgreSQL) and a warning is
# logged.  This keeps the pipeline resilient to dataset changes.
#
# HOW TO DISCOVER THE CORRECT KEYS:
#   1. Run  `python src/extract_api.py`  with your chosen SOCRATA_DATASET_ID.
#   2. Run  `src.extract_api.profile_api_data("/tmp/raw_macro_data.parquet")`.
#   3. The profiled column names are the keys you need below.
#
# Common DANE IPC columns (dataset ≈ er9f-6p48):
#   variaci_n_anual, variaci_n_mensual, variaci_n_a_o_corrido, ndice
# Common DANE Poverty columns (dataset ≈ hfcb-tij6):
#   incidencia, incidencia_pobreza_extrema

_MACRO_COLUMN_MAP: dict[str, tuple[str, str]] = {
    # Parquet col (from API)             DB col                  dtype
    "valor_miles_de_millones_de":        ("pib_miles_millones",   "float32"),
}

# DB-side macro column names (must match ALTER TABLE in alter_schema.sql)
_MACRO_DB_COLS: list[str] = ["pib_miles_millones"]

# ─── SQL: ensure unique constraints (idempotent, run before first INSERT) ────
# These indexes are the prerequisite for INSERT … ON CONFLICT DO NOTHING.
# The original create_tables.sql does not define them; we add them here so that
# the load module is self-sufficient and can be run against a freshly migrated DB.
_UNIQUE_INDEX_DDL: list[str] = [
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_period
        ON dim_period (vigencia, periodo_otorgamiento)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_geography
        ON dim_geography (codigo_departamento, departamento, categoria_municipio)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_program
        ON dim_program (sector_ies, nivel_formacion, modalidad_linea, modalidad_credito)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_student
        ON dim_student_profile (sexo_al_nacer, estrato_socioeconomico)
    """,
]


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _ensure_unique_constraints(conn: Connection) -> None:
    """Creates UNIQUE indexes on dimension natural keys if they do not exist.

    Safe to call on every pipeline run — all statements use ``IF NOT EXISTS``.

    Args:
        conn: Active SQLAlchemy ``Connection`` within an open transaction.
    """
    for ddl in _UNIQUE_INDEX_DDL:
        conn.execute(text(ddl))
    logger.info("Unique indexes verified (CREATE … IF NOT EXISTS).")


def _extract_dimension(
    df: pd.DataFrame,
    col_map: dict[str, tuple[str, str]],
) -> pd.DataFrame:
    """Extracts and renames the unique dimension rows from the merged DataFrame.

    Args:
        df: Flat merged DataFrame from the validated Parquet.
        col_map: Mapping from source column names to ``(db_col_name, dtype)``.

    Returns:
        De-duplicated DataFrame with DB-aligned column names and correct dtypes.

    Raises:
        KeyError: If a required source column is absent from ``df``.
    """
    src_cols = list(col_map.keys())
    missing = [c for c in src_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Source columns missing from Parquet: {missing}")

    dim = df[src_cols].drop_duplicates().copy()

    rename = {src: db_col for src, (db_col, _) in col_map.items()}
    dim = dim.rename(columns=rename)

    for src_col, (db_col, dtype) in col_map.items():
        if dtype == "int32":
            dim[db_col] = pd.to_numeric(dim[db_col], errors="coerce").astype("Int32")
        else:
            dim[db_col] = dim[db_col].astype(str).str.strip()

    return dim.dropna()   # drop rows where a required key became NaN after cast


def _upsert_dimension(
    conn: Connection,
    table: str,
    dim_df: pd.DataFrame,
    natural_keys: list[str],
    chunk_size: int = CHUNK_SIZE,
) -> pd.DataFrame:
    """Inserts new dimension rows and returns all rows with DB-generated SKs.

    Uses ``INSERT … ON CONFLICT (natural_keys) DO NOTHING`` to skip rows that
    already exist, then ``SELECT *`` to retrieve the authoritative surrogate
    keys for ALL rows (new + pre-existing).

    Args:
        conn: Active SQLAlchemy ``Connection``.
        table: Target dimension table name.
        dim_df: De-duplicated DataFrame with DB-aligned column names.
        natural_keys: Column names that form the unique constraint.
        chunk_size: Rows per INSERT batch.

    Returns:
        DataFrame with all columns from the DB table, including the SK column.
    """
    cols = list(dim_df.columns)
    col_list = ", ".join(cols)
    bind_list = ", ".join(f":{c}" for c in cols)
    conflict_cols = ", ".join(natural_keys)

    insert_sql = text(f"""
        INSERT INTO {table} ({col_list})
        VALUES ({bind_list})
        ON CONFLICT ({conflict_cols}) DO NOTHING
    """)

    total_inserted = 0
    for start in range(0, len(dim_df), chunk_size):
        chunk = dim_df.iloc[start : start + chunk_size]
        records = chunk.to_dict(orient="records")
        result = conn.execute(insert_sql, records)
        total_inserted += result.rowcount

    logger.info(
        "  %s | %d new rows inserted | %d total in source",
        table,
        total_inserted,
        len(dim_df),
    )

    # Retrieve all rows with DB-generated SKs
    db_df = pd.read_sql(f"SELECT * FROM {table}", conn)
    return db_df


def _resolve_macro_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Maps API-originated macro columns from the Parquet to DB column names.

    Iterates over ``_MACRO_COLUMN_MAP`` looking for matching columns in the
    source DataFrame.  When a match is found, the column is renamed to its DB
    equivalent and cast to ``float32``.  Columns not found in the source are
    created and filled with ``NaN`` (→ ``NULL`` in PostgreSQL).

    If multiple Parquet columns map to the same DB column (aliases), the first
    one found wins.

    Args:
        df: Flat merged DataFrame (columns are a mix of uppercase ICETEX names
            and lowercase API names produced by ``transform_and_merge.py``).

    Returns:
        ``df`` with macro columns renamed to their DB equivalents.  Missing
        macro columns are present as all-NaN float32 Series.
    """
    resolved: set[str] = set()  # DB column names already resolved

    for parquet_col, (db_col, dtype) in _MACRO_COLUMN_MAP.items():
        if db_col in resolved:
            continue  # already resolved via a prior alias
        if parquet_col in df.columns:
            df = df.rename(columns={parquet_col: db_col})
            df[db_col] = pd.to_numeric(df[db_col], errors="coerce").astype(dtype)
            resolved.add(db_col)
            logger.info(
                "  macro-map | Parquet '%s' → DB '%s' (dtype=%s)",
                parquet_col,
                db_col,
                dtype,
            )

    # Fill any DB macro cols that had no matching Parquet column with NaN
    for db_col in _MACRO_DB_COLS:
        if db_col not in resolved:
            df[db_col] = pd.Series(pd.NA, index=df.index, dtype="Float32")
            logger.warning(
                "  macro-map | No Parquet source found for DB col '%s' — "
                "filled with NULL.  Run profile_api_data() to inspect "
                "available API columns.",
                db_col,
            )

    return df


def _build_fact_df(
    source_df: pd.DataFrame,
    db_period: pd.DataFrame,
    db_geo: pd.DataFrame,
    db_program: pd.DataFrame,
    db_student: pd.DataFrame,
) -> pd.DataFrame:
    """Joins DB surrogate keys back onto the source rows to form the fact table.

    Each dimension merge is a LEFT JOIN keyed on natural business columns.  Any
    source row that fails to resolve a SK will have a null in that SK column —
    these are caught and logged before insertion.

    Args:
        source_df: Full flat merged DataFrame (one row per credit cohort).
        db_period: dim_period rows with DB-generated ``sk_period``.
        db_geo: dim_geography rows with DB-generated ``sk_geography``.
        db_program: dim_program rows with DB-generated ``sk_program``.
        db_student: dim_student_profile rows with DB-generated ``sk_student_profile``.

    Returns:
        DataFrame with columns required by the ``fact_credits`` table,
        including macroeconomic indicator columns (nullable).
    """
    # Rename source columns to DB names for the merge keys
    fact = source_df.rename(columns={
        src: db for src, (db, _) in {
            **_PERIOD_MAP, **_GEO_MAP, **_PROGRAM_MAP, **_STUDENT_MAP, **_FACT_MEASURE_MAP
        }.items()
        if src in source_df.columns
    }).copy()

    # ── Resolve macroeconomic columns from the API side of the Parquet ────────
    logger.info("Resolving macroeconomic columns from merged Parquet…")
    fact = _resolve_macro_columns(fact)

    # Cast types consistently
    fact["codigo_departamento"] = pd.to_numeric(
        fact["codigo_departamento"], errors="coerce"
    ).astype("Int32")
    fact["estrato_socioeconomico"] = pd.to_numeric(
        fact["estrato_socioeconomico"], errors="coerce"
    ).astype("Int32")
    fact["total_nuevos_beneficiarios"] = pd.to_numeric(
        fact["total_nuevos_beneficiarios"], errors="coerce"
    ).astype("Int32")

    # ── Join SKs from each dimension ─────────────────────────────────────────
    fact = fact.merge(
        db_period[["sk_period", "vigencia", "periodo_otorgamiento"]],
        on=["vigencia", "periodo_otorgamiento"],
        how="left",
    )
    fact = fact.merge(
        db_geo[["sk_geography", "codigo_departamento", "departamento", "categoria_municipio"]],
        on=["codigo_departamento", "departamento", "categoria_municipio"],
        how="left",
    )
    fact = fact.merge(
        db_program[["sk_program", "sector_ies", "nivel_formacion", "modalidad_linea", "modalidad_credito"]],
        on=["sector_ies", "nivel_formacion", "modalidad_linea", "modalidad_credito"],
        how="left",
    )
    fact = fact.merge(
        db_student[["sk_student_profile", "sexo_al_nacer", "estrato_socioeconomico"]],
        on=["sexo_al_nacer", "estrato_socioeconomico"],
        how="left",
    )

    sk_cols = ["sk_period", "sk_geography", "sk_program", "sk_student_profile"]
    null_sks = fact[sk_cols].isna().any(axis=1).sum()
    if null_sks > 0:
        logger.warning(
            "%d fact rows have at least one unresolved SK — they will be excluded "
            "from the load to preserve FK integrity.",
            null_sks,
        )
        fact = fact.dropna(subset=sk_cols)

    # ── Final column selection (core + macro) ─────────────────────────────────
    fact_cols = [
        "sk_period", "sk_geography", "sk_program", "sk_student_profile",
        "rango_valor_desembolsado", "total_nuevos_beneficiarios",
    ] + _MACRO_DB_COLS

    missing_fact_cols = [c for c in fact_cols if c not in fact.columns]
    if missing_fact_cols:
        raise KeyError(f"Fact columns not resolved: {missing_fact_cols}")

    # Replace pandas NA with None for SQLAlchemy parameterised INSERT
    result = fact[fact_cols].copy()
    for mcol in _MACRO_DB_COLS:
        result[mcol] = result[mcol].where(result[mcol].notna(), other=None)

    return result


def _load_fact_credits(
    conn: Connection,
    fact_df: pd.DataFrame,
    vigencias: list[int],
    chunk_size: int = CHUNK_SIZE,
) -> None:
    """Watermark DELETE then chunked INSERT for fact_credits.

    Deletes all existing fact rows for the vigencias being loaded, then inserts
    the fresh rows in chunks.  Both operations run inside the caller's
    transaction, guaranteeing atomicity.

    Args:
        conn: Active SQLAlchemy ``Connection``.
        fact_df: DataFrame with the final fact_credits columns.
        vigencias: List of year values being reloaded (the watermark boundary).
        chunk_size: Rows per INSERT batch.
    """
    # Step 1: Watermark DELETE — only affects the years in the current load
    delete_sql = text("""
        DELETE FROM fact_credits
        WHERE sk_period IN (
            SELECT sk_period FROM dim_period WHERE vigencia = ANY(:years)
        )
    """)
    del_result = conn.execute(delete_sql, {"years": list(vigencias)})
    logger.info(
        "fact_credits | watermark DELETE | years=%s | rows_deleted=%d",
        sorted(vigencias),
        del_result.rowcount,
    )

    # Step 2: Chunked INSERT (core + macro columns)
    insert_sql = text("""
        INSERT INTO fact_credits
            (sk_period, sk_geography, sk_program, sk_student_profile,
             rango_valor_desembolsado, total_nuevos_beneficiarios,
             inflacion_anual, ipc_indice, pobreza_monetaria)
        VALUES
            (:sk_period, :sk_geography, :sk_program, :sk_student_profile,
             :rango_valor_desembolsado, :total_nuevos_beneficiarios,
             :inflacion_anual, :ipc_indice, :pobreza_monetaria)
    """)

    total_rows = len(fact_df)
    rows_inserted = 0
    for start in range(0, total_rows, chunk_size):
        chunk = fact_df.iloc[start : start + chunk_size]
        conn.execute(insert_sql, chunk.to_dict(orient="records"))
        rows_inserted += len(chunk)
        logger.info(
            "fact_credits | inserted %d / %d rows",
            rows_inserted,
            total_rows,
        )

    logger.info("fact_credits | load complete | total_rows=%d", total_rows)


# ─── Public interface ─────────────────────────────────────────────────────────

def load_to_datawarehouse(parquet_path: str, db_uri: str) -> None:
    """Loads the validated merged Parquet into the PostgreSQL star schema.

    Implements two idempotency strategies (see module docstring for rationale):
    * Dimensions → ``INSERT … ON CONFLICT DO NOTHING``
    * Fact table → Year-watermark ``DELETE`` + chunked ``INSERT``

    The entire operation runs inside a single transaction.  A failure at any
    point rolls back to the pre-load state, leaving the DW in a consistent
    snapshot.

    Args:
        parquet_path: Absolute path to the validated ``.parquet`` file produced
            by ``validate_data.run_data_quality_checks()``.
        db_uri: SQLAlchemy connection URI for the ``icetex_ods_dw`` database,
            e.g. ``postgresql+psycopg2://icetex:icetex@postgres-dw:5432/icetex_ods_dw``.

    Raises:
        FileNotFoundError: If ``parquet_path`` does not exist on disk.
        KeyError: If a required source column is absent from the Parquet.
        sqlalchemy.exc.SQLAlchemyError: On any unrecoverable DB error (rolled back).
        Exception: Logged with full stack trace before re-raising.
    """
    logger.info("=" * 68)
    logger.info("LOAD PHASE — PostgreSQL Data Warehouse")
    logger.info("=" * 68)

    parquet_file = Path(parquet_path)
    if not parquet_file.exists():
        raise FileNotFoundError(f"Validated Parquet not found: {parquet_file}")

    try:
        # ── Read source ───────────────────────────────────────────────────────
        logger.info("[1/6] Reading Parquet: %s", parquet_file)
        df = pd.read_parquet(parquet_file, engine="pyarrow")
        logger.info("Source shape: %d rows × %d cols", *df.shape)

        # Identify vigencias for the fact watermark
        vigencias: list[int] = (
            pd.to_numeric(df["VIGENCIA"], errors="coerce")
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
        logger.info("Vigencias in this load: %s", sorted(vigencias))

        engine: Engine = create_engine(
            db_uri,
            pool_pre_ping=True,    # recycles stale connections
            pool_size=2,
            max_overflow=0,
        )

        with engine.begin() as conn:   # single atomic transaction
            # ── 0. Ensure unique constraints ──────────────────────────────────
            logger.info("[2/6] Ensuring UNIQUE indexes on dimension natural keys…")
            _ensure_unique_constraints(conn)

            # ── 1. Dimensions ─────────────────────────────────────────────────
            logger.info("[3/6] Upserting dimensions…")

            dim_period_src  = _extract_dimension(df, _PERIOD_MAP)
            dim_geo_src     = _extract_dimension(df, _GEO_MAP)
            dim_program_src = _extract_dimension(df, _PROGRAM_MAP)
            dim_student_src = _extract_dimension(df, _STUDENT_MAP)

            db_period  = _upsert_dimension(conn, "dim_period",         dim_period_src,  ["vigencia", "periodo_otorgamiento"])
            db_geo     = _upsert_dimension(conn, "dim_geography",      dim_geo_src,     ["codigo_departamento", "departamento", "categoria_municipio"])
            db_program = _upsert_dimension(conn, "dim_program",        dim_program_src, ["sector_ies", "nivel_formacion", "modalidad_linea", "modalidad_credito"])
            db_student = _upsert_dimension(conn, "dim_student_profile", dim_student_src, ["sexo_al_nacer", "estrato_socioeconomico"])

            # ── 2. Build fact DataFrame with DB-generated SKs ──────────────────
            logger.info("[4/6] Resolving DB surrogate keys for fact table…")
            fact_df = _build_fact_df(df, db_period, db_geo, db_program, db_student)
            logger.info("Fact rows ready for insert: %d", len(fact_df))

            # ── 3. Watermark DELETE + chunked INSERT ───────────────────────────
            logger.info("[5/6] Loading fact_credits (watermark DELETE + INSERT)…")
            _load_fact_credits(conn, fact_df, vigencias)

        logger.info("[6/6] Transaction committed successfully.")
        logger.info("=" * 68)

    except Exception:
        logger.error(
            "Load failed — transaction rolled back.\n%s",
            traceback.format_exc(),
        )
        raise
    finally:
        try:
            engine.dispose()
        except Exception:
            pass


# ─── Local smoke-test ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    import sys

    _PARQUET = os.getenv("MERGED_PARQUET_PATH", "/tmp/merged_data.parquet")
    _DB_URI = os.getenv(
        "ICETEX_DW_URI",
        "postgresql+psycopg2://icetex:icetex@localhost:5433/icetex_ods_dw",
    )
    try:
        load_to_datawarehouse(_PARQUET, _DB_URI)
    except Exception as exc:
        logger.error("Smoke-test failed: %s", exc)
        sys.exit(1)
