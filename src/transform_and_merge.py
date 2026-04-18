"""
transform_and_merge.py
======================
Phase 1.3 — ETL Pipeline: Geographic Fuzzy-Matching & Star Schema Integration

Harmonises the ICETEX cohorte dataset with DANE macroeconomic indicators
(IPC / poverty) extracted via the Socrata API, using fuzzy string matching
to bridge inconsistent departmental naming conventions across sources.

Key design decisions
---------------------
* **Left table = ICETEX** — every credit record is preserved in the output.
  Macro columns are nullable before imputation; no fact row is dropped.
* **Fuzzy matching at unique-value level** — ``_map_geography_keys`` operates
  on ≤ 33 unique Colombian department names, not on every row. The resulting
  dict is applied via a vectorised ``Series.map()`` call.
* **Annual aggregation before merge** — Socrata IPC data may arrive at monthly
  or quarterly granularity. It is reduced to one (year × department) row using
  column-wise ``mean`` before the join.
* **Year-median imputation** — unmatched rows receive the national median for
  that calendar year, preserving all ICETEX records without distorting the
  analytical distribution.

Airflow XCom pattern
---------------------
The function returns a ``str`` path to the output Parquet. The PythonOperator
pushes this string via XCom; the downstream Great Expectations task receives
it with ``ti.xcom_pull(task_ids='transform_and_merge_data')``.

ICETEX source columns used as join keys (from ``transform.clean_and_standardize``)
------------------------------------------------------------------------------------
* ``VIGENCIA``            — calendar year (int, e.g. 2018)
* ``DEPARTAMENTO DE ORIGEN`` — department name, uppercase and stripped
"""

from __future__ import annotations

import logging
import traceback
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd
from rapidfuzz import fuzz, process

# ─── Module logger ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-26s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Column name constants ────────────────────────────────────────────────────
# ICETEX — names produced by transform.clean_and_standardize
ICETEX_YEAR_COL: str = "VIGENCIA"
ICETEX_GEO_COL: str = "DEPARTAMENTO DE ORIGEN"

# API — names set in extract_api.py (YEAR_COLUMN / _GEO_COLUMN_CANDIDATES)
# Override via function arguments if the Socrata dataset uses different names.
API_YEAR_COL: str = "a_o"          # DANE encoding quirk for "año"
API_GEO_COL: str = "departamento"  # first candidate in _GEO_COLUMN_CANDIDATES

# Internal working column: fuzzy-mapped dept name inside the API DataFrame
_MAPPED_GEO_COL: str = "_dept_icetex_mapped"

# ─── Tuning parameters ───────────────────────────────────────────────────────
# token_sort_ratio score (0–100) below which a geographic match is rejected.
# Scores below this → the ICETEX row is imputed with the national year-median.
FUZZY_THRESHOLD: int = 85

# ─── I/O ────────────────────────────────────────────────────────────────────
DEFAULT_MERGED_PATH: str = "/tmp/merged_data.parquet"


# ─── Private helpers (internal — not part of the public Airflow task API) ────

def _validate_columns(
    df: pd.DataFrame,
    required: list[str],
    source: str,
) -> None:
    """Raises ``KeyError`` if any required column is absent from a DataFrame.

    Args:
        df: DataFrame to validate.
        required: Column names that must exist.
        source: Label included in the error message (e.g. ``"ICETEX"``).

    Raises:
        KeyError: Listing the missing column names and the available columns.
    """
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(
            f"[{source}] Missing required columns: {missing}. "
            f"Available: {list(df.columns)}"
        )


def _clean_icetex_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Applies the minimal cleaning required on the ICETEX merge keys.

    This is a targeted subset of ``transform.clean_and_standardize`` scoped
    exclusively to the two columns used as join keys.  Keeping it isolated
    avoids coupling this module to the full transform pipeline and allows
    ``transform_and_merge_data`` to receive a **raw** DataFrame directly from
    ``extract_csv``.

    Operations applied:
        * Strip leading/trailing whitespace from all column names.
        * ``VIGENCIA``: remove thousands separator (``"2,015"`` → ``2015``),
          cast to ``int32``.
        * ``DEPARTAMENTO DE ORIGEN``: coerce to ``str``, strip, uppercase.

    Args:
        df: Raw ICETEX DataFrame as returned by ``extract.extract_csv()``.

    Returns:
        Copy of ``df`` with cleaned ``VIGENCIA`` and ``DEPARTAMENTO DE ORIGEN``.
    """
    df = df.copy()
    df.columns = df.columns.str.strip()

    df[ICETEX_YEAR_COL] = (
        df[ICETEX_YEAR_COL]
        .astype(str)
        .str.replace(",", "", regex=False)
        .astype("int32")
    )
    df[ICETEX_GEO_COL] = (
        df[ICETEX_GEO_COL]
        .astype(str)
        .str.strip()
        .str.upper()
    )
    return df


def _remove_accents(text: str) -> str:
    """Strips diacritical marks from a Unicode string.

    Used only during fuzzy-matching comparisons so that ``"BOGOTÁ"``
    and ``"BOGOTA"`` are treated as equivalent.  The canonical accented
    form is **not** modified in the stored data columns.

    Args:
        text: Input string, e.g. ``"CÓRDOBA"``.

    Returns:
        Accent-free version, e.g. ``"CORDOBA"``.
    """
    return "".join(
        c
        for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def _map_geography_keys(
    source_series: pd.Series,
    target_series: pd.Series,
    threshold: int = FUZZY_THRESHOLD,
) -> dict[str, Optional[str]]:
    """Builds a fuzzy mapping from dirty API department names to canonical ICETEX names.

    **Why token_sort_ratio?** It tokenises both strings, sorts tokens
    alphabetically, and then applies standard Levenshtein ratio.  This
    correctly handles token-order variations common in Colombian data, e.g.:
    ``"NORTE SANTANDER"`` → ``"NORTE DE SANTANDER"``.

    **Complexity:** operates on *unique* values only (≤ 33 Colombian
    departments), so the number of scorer calls is negligible regardless of
    how many rows the DataFrames contain.

    Args:
        source_series: ``departamento`` column from the API DataFrame.
            Values may be dirty: ``"VALLE"``, ``"BOGOTA"``, ``"N. SANTANDER"``.
        target_series: ``DEPARTAMENTO DE ORIGEN`` column from the cleaned
            ICETEX DataFrame — the canonical reference set.
        threshold: Minimum ``token_sort_ratio`` score (0–100) to accept a
            match.  Pairs scoring below this are mapped to ``None`` and later
            imputed via the year-median.

    Returns:
        Dict mapping each unique API department name to its canonical ICETEX
        equivalent (or ``None`` if no match meets the threshold).

        Example::

            {
                "VALLE":      "VALLE DEL CAUCA",   # score 91
                "BOGOTA":     "BOGOTÁ D.C.",        # score 87
                "SAN ANDRES": None,                  # score 62 < 85
            }
    """
    # Build normalised lists — accent removal + uppercase + strip
    source_unique_raw: list[str] = source_series.dropna().astype(str).unique().tolist()
    source_unique_norm: list[str] = [
        _remove_accents(v.upper().strip()) for v in source_unique_raw
    ]

    target_unique_raw: list[str] = target_series.dropna().astype(str).unique().tolist()
    target_unique_norm: list[str] = [
        _remove_accents(v) for v in target_unique_raw
    ]

    logger.info(
        "Fuzzy matching | %d unique API depts → %d unique ICETEX depts",
        len(source_unique_raw),
        len(target_unique_raw),
    )

    mapping: dict[str, Optional[str]] = {}

    for raw_val, norm_val in zip(source_unique_raw, source_unique_norm):
        result = process.extractOne(
            norm_val,
            target_unique_norm,
            scorer=fuzz.token_sort_ratio,
        )
        if result is None:
            logger.warning(
                "  geo-map | %-35s → NO CANDIDATE FOUND — will be imputed",
                raw_val,
            )
            mapping[raw_val] = None
            continue

        best_norm_match, score, best_idx = result
        best_raw_match: str = target_unique_raw[best_idx]

        if score >= threshold:
            mapping[raw_val] = best_raw_match
            logger.info(
                "  geo-map | %-35s → %-35s (score=%d)",
                raw_val,
                best_raw_match,
                score,
            )
        else:
            mapping[raw_val] = None
            logger.warning(
                "  geo-map | %-35s → UNMATCHED  best='%s' score=%d < threshold=%d",
                raw_val,
                best_raw_match,
                score,
                threshold,
            )

    return mapping


def _aggregate_api_to_annual(
    df: pd.DataFrame,
    year_col: str,
    geo_col: str,
) -> pd.DataFrame:
    """Reduces the API DataFrame to one row per (year × department).

    The Socrata IPC dataset may contain monthly or quarterly records.  This
    function computes the column-wise arithmetic mean within each
    (year, department) group, producing the annual representative value
    required to join against the ICETEX fact data at annual granularity.

    ``mean`` is preferred over ``last`` because it is robust to missing months
    within a year (a single recorded quarter still produces a meaningful value).

    Args:
        df: API DataFrame with already-mapped geographic column
            (``_MAPPED_GEO_COL``).  Rows where ``geo_col`` is ``NaN`` must be
            dropped by the caller before passing here.
        year_col: Name of the integer year column.
        geo_col: Name of the mapped department column to group by.

    Returns:
        Aggregated DataFrame; one row per ``(year_col, geo_col)`` pair.
        Only numeric columns (excluding the year key) are aggregated.
    """
    numeric_cols = (
        df.select_dtypes(include="number")
        .columns
        .difference([year_col])
        .tolist()
    )
    agg_spec: dict[str, str] = {col: "mean" for col in numeric_cols}

    aggregated = (
        df.groupby([year_col, geo_col], observed=True)
        .agg(agg_spec)
        .reset_index()
    )
    logger.info(
        "API aggregated to annual level | %d rows × %d cols | years: %s",
        len(aggregated),
        len(aggregated.columns),
        sorted(aggregated[year_col].unique()),
    )
    return aggregated


def _impute_with_year_median(
    df: pd.DataFrame,
    macro_cols: list[str],
    year_col: str,
) -> pd.DataFrame:
    """Fills nulls in macro columns using the national annual median.

    Uses ``GroupBy.transform('median')`` — a single vectorised pass per
    column with no Python-level row iteration.

    **Why median over mean?** IPC distributions during high-inflation years
    can be right-skewed.  The median is robust to extreme departmental outliers
    and avoids artificially inflating the imputed value for underrepresented
    departments.

    Args:
        df: Merged DataFrame containing both ICETEX and macro columns.
        macro_cols: Column names from the API side to impute.
        year_col: Year column used to segment the annual median groups.

    Returns:
        Copy of ``df`` with nulls in ``macro_cols`` filled by year-median.
        Cells where the entire year-group is null (no API data for that year)
        remain ``NaN`` and are flagged in logs.
    """
    df = df.copy()
    for col in macro_cols:
        null_before = int(df[col].isna().sum())
        if null_before == 0:
            continue

        year_median: pd.Series = df.groupby(year_col)[col].transform("median")
        df[col] = df[col].fillna(year_median)

        null_after = int(df[col].isna().sum())
        filled = null_before - null_after
        logger.info(
            "  impute | %-40s  filled=%d  still_null=%d "
            "(year-group fully absent from API)",
            col,
            filled,
            null_after,
        )
    return df


# ─── Public interface ─────────────────────────────────────────────────────────

def transform_and_merge_data(
    icetex_df: pd.DataFrame,
    api_parquet_path: str,
    output_path: str = DEFAULT_MERGED_PATH,
    api_year_col: str = API_YEAR_COL,
    api_geo_col: str = API_GEO_COL,
    fuzzy_threshold: int = FUZZY_THRESHOLD,
) -> str:
    """Cleans, fuzzy-matches geographic keys, and LEFT JOINs ICETEX with macro data.

    Full pipeline (8 steps):

    1. Validate and clean ICETEX merge keys (``VIGENCIA``, ``DEPARTAMENTO DE ORIGEN``).
    2. Load and validate the API Parquet file.
    3. Cast the API year column to ``int32`` to align dtypes before the join.
    4. Run ``_map_geography_keys`` (``token_sort_ratio``) to produce a
       dirty-name → canonical-name mapping dict.
    5. Apply the mapping via ``Series.map()`` (vectorised) and aggregate the
       API data to one row per (year × mapped department).
    6. ``LEFT JOIN`` — ICETEX is the left table; no credit record is dropped.
    7. Audit unmatched rows (all macro cols null after join); log a warning if
       the ratio exceeds ~5 %, indicating a fuzzy-matching failure.
    8. Impute remaining nulls with the national annual median per macro column.

    Args:
        icetex_df: Raw ICETEX DataFrame as returned by ``extract.extract_csv()``.
            Must contain ``VIGENCIA`` and ``DEPARTAMENTO DE ORIGEN`` at minimum.
        api_parquet_path: Absolute path to the raw macro Parquet produced by
            ``extract_api.extract_macroeconomic_data()``.
        output_path: Absolute path for the merged ``.parquet`` output.
            Parent directories are created automatically.
        api_year_col: Year column name in the API data (default: ``"a_o"``).
            Override if the Socrata dataset uses a different name.
        api_geo_col: Department column name in the API data
            (default: ``"departamento"``).
        fuzzy_threshold: Minimum ``token_sort_ratio`` score to accept a
            geographic match (default: ``85``).

    Returns:
        Absolute path to the merged Parquet file.  Pass this string as the
        Airflow XCom value to the downstream Great Expectations task.

    Raises:
        FileNotFoundError: If ``api_parquet_path`` does not exist on disk.
        KeyError: If a required join-key column is absent from either source.
        RuntimeError: If the merged DataFrame is empty after the join.
        Exception: Any unrecoverable error is logged with a full stack trace
            before re-raising.
    """
    logger.info("=" * 72)
    logger.info("TRANSFORM & MERGE  —  ICETEX × Macroeconomic API Data")
    logger.info("=" * 72)

    try:
        # ── Step 1: Validate and clean ICETEX merge keys ──────────────────────
        logger.info("[1/8] Cleaning ICETEX merge keys…")
        _validate_columns(
            icetex_df,
            required=[ICETEX_YEAR_COL, ICETEX_GEO_COL],
            source="ICETEX",
        )
        icetex = _clean_icetex_keys(icetex_df)
        logger.info(
            "ICETEX ready | rows=%d | years=%s | unique_depts=%d",
            len(icetex),
            sorted(icetex[ICETEX_YEAR_COL].unique().tolist()),
            icetex[ICETEX_GEO_COL].nunique(),
        )

        # ── Step 2: Load and validate the API Parquet ─────────────────────────
        logger.info("[2/8] Loading API Parquet from '%s'…", api_parquet_path)
        api_path = Path(api_parquet_path)
        if not api_path.exists():
            raise FileNotFoundError(f"API Parquet not found: {api_path}")

        api_df = pd.read_parquet(api_path, engine="pyarrow")
        _validate_columns(
            api_df,
            required=[api_year_col, api_geo_col],
            source="API",
        )
        logger.info(
            "API Parquet loaded | rows=%d | cols=%d | dtypes: %s",
            len(api_df),
            len(api_df.columns),
            dict(api_df.dtypes),
        )

        # ── Step 3: Align API year column dtype → int32 ───────────────────────
        logger.info("[3/8] Casting API year column '%s' → int32…", api_year_col)
        api_df[api_year_col] = pd.to_numeric(api_df[api_year_col], errors="coerce")

        invalid_year_mask = api_df[api_year_col].isna()
        if invalid_year_mask.any():
            logger.warning(
                "Dropping %d API rows with unparseable year values.",
                int(invalid_year_mask.sum()),
            )
            api_df = api_df.loc[~invalid_year_mask].copy()

        api_df[api_year_col] = api_df[api_year_col].astype("int32")
        logger.info(
            "API year range after cast: %d – %d",
            api_df[api_year_col].min(),
            api_df[api_year_col].max(),
        )

        # ── Step 4: Fuzzy geographic matching ─────────────────────────────────
        logger.info(
            "[4/8] Running fuzzy matching (scorer=token_sort_ratio, threshold=%d)…",
            fuzzy_threshold,
        )
        geo_mapping = _map_geography_keys(
            source_series=api_df[api_geo_col],
            target_series=icetex[ICETEX_GEO_COL],
            threshold=fuzzy_threshold,
        )

        n_matched = sum(v is not None for v in geo_mapping.values())
        n_unmatched_depts = sum(v is None for v in geo_mapping.values())
        logger.info(
            "Mapping result | matched=%d  unmatched=%d out of %d unique API depts",
            n_matched,
            n_unmatched_depts,
            len(geo_mapping),
        )
        if n_unmatched_depts:
            logger.warning(
                "Unmatched API depts (will be excluded from the join): %s",
                [k for k, v in geo_mapping.items() if v is None],
            )

        # Apply mapping via vectorised dict lookup — no iterrows
        api_df[_MAPPED_GEO_COL] = api_df[api_geo_col].astype(str).map(geo_mapping)

        # ── Step 5: Aggregate API to annual × department level ────────────────
        logger.info("[5/8] Aggregating API data to annual granularity…")
        api_mapped = api_df.dropna(subset=[_MAPPED_GEO_COL])

        if api_mapped.empty:
            raise RuntimeError(
                "All API departments failed to match against ICETEX references. "
                "Lower 'fuzzy_threshold' or verify 'api_geo_col' column name."
            )

        api_annual = _aggregate_api_to_annual(
            df=api_mapped,
            year_col=api_year_col,
            geo_col=_MAPPED_GEO_COL,
        )

        # Rename to match ICETEX join keys so the merge call is clean
        api_annual = api_annual.rename(
            columns={
                api_year_col: ICETEX_YEAR_COL,
                _MAPPED_GEO_COL: ICETEX_GEO_COL,
            }
        )

        macro_cols: list[str] = [
            c for c in api_annual.columns
            if c not in {ICETEX_YEAR_COL, ICETEX_GEO_COL}
        ]
        logger.info("Macro columns entering the merge: %s", macro_cols)

        # ── Step 6: LEFT JOIN ─────────────────────────────────────────────────
        logger.info(
            "[6/8] LEFT JOIN on %s + %s…",
            ICETEX_YEAR_COL,
            ICETEX_GEO_COL,
        )
        merged = icetex.merge(
            api_annual,
            on=[ICETEX_YEAR_COL, ICETEX_GEO_COL],
            how="left",
            suffixes=("", "_api"),  # guard against accidental column collisions
        )

        if merged.empty:
            raise RuntimeError(
                "Merged DataFrame is empty. "
                "Verify that VIGENCIA ranges overlap between ICETEX and API data."
            )
        logger.info(
            "Merge complete | output: %d rows × %d cols",
            len(merged),
            len(merged.columns),
        )

        # ── Step 7: Post-merge null audit ─────────────────────────────────────
        logger.info("[7/8] Auditing unmatched rows post-merge…")
        if macro_cols:
            # A row is "unmatched" when ALL macro columns are simultaneously null
            unmatched_mask: pd.Series = merged[macro_cols].isna().all(axis=1)
            n_unmatched_rows = int(unmatched_mask.sum())
            pct_unmatched = n_unmatched_rows / len(merged) * 100

            if n_unmatched_rows > 0:
                logger.warning(
                    "Unmatched ICETEX rows (all macro cols null): %d / %d (%.2f%%). "
                    "If >5%%, inspect the geo-mapping log above.",
                    n_unmatched_rows,
                    len(merged),
                    pct_unmatched,
                )
                top_unmatched = (
                    merged.loc[unmatched_mask, ICETEX_GEO_COL]
                    .value_counts()
                    .head(10)
                )
                logger.warning(
                    "Top departments in unmatched rows:\n%s",
                    top_unmatched.to_string(),
                )
            else:
                logger.info(
                    "Null audit passed — 0 unmatched rows (100%% geo coverage)."
                )

        # ── Step 8: Impute nulls with national year-median ────────────────────
        logger.info("[8/8] Imputing remaining nulls with year-median…")
        merged = _impute_with_year_median(
            df=merged,
            macro_cols=macro_cols,
            year_col=ICETEX_YEAR_COL,
        )

        residual_nulls = int(merged[macro_cols].isna().sum().sum()) if macro_cols else 0
        if residual_nulls > 0:
            logger.warning(
                "%d null values remain after imputation "
                "(year-groups with zero API coverage). "
                "Great Expectations will flag these in the next task.",
                residual_nulls,
            )
        else:
            logger.info("All macro nulls resolved — DataFrame is imputation-complete.")

        # ── Persist ───────────────────────────────────────────────────────────
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(
            output_file,
            index=False,
            engine="pyarrow",
            compression="snappy",
        )

        file_mb = output_file.stat().st_size / 1_048_576
        logger.info(
            "Parquet written | path=%s | %.2f MB | rows=%d | cols=%d",
            output_file,
            file_mb,
            len(merged),
            len(merged.columns),
        )
        logger.info("=" * 72)
        return str(output_file)

    except Exception:
        logger.error(
            "Unrecoverable error in transform_and_merge_data.\n%s",
            traceback.format_exc(),
        )
        raise


# ─── Local test harness ────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from extract import extract_csv  # type: ignore[import-untyped]

    _BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _CSV = os.path.join(_BASE, "data", "raw", "Créditos_Otorgados._20260304.csv")
    _API_PARQUET = os.getenv("API_PARQUET_PATH", "/tmp/raw_macro_data.parquet")

    _raw_icetex = extract_csv(_CSV)
    _merged_path = transform_and_merge_data(
        icetex_df=_raw_icetex,
        api_parquet_path=_API_PARQUET,
    )
    logger.info("Test run complete. Merged output at: %s", _merged_path)
