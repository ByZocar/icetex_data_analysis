"""
extract_api.py
==============
Phase 1 — ETL Pipeline: Macroeconomic Data Extraction via Socrata API

Extracts IPC (Consumer Price Index) or Monetary Poverty data published by
DANE on Colombia's Open Data portal (datos.gov.co) using the sodapy Socrata
client. Outputs a Parquet file to avoid DataFrame serialisation overhead in
Airflow XCom (only the file path is returned / passed between tasks).

Standalone usage::

    export SOCRATA_APP_TOKEN="your_token"
    python src/extract_api.py

Airflow PythonOperator usage::

    from src.extract_api import extract_macroeconomic_data, profile_api_data

Dependencies::

    pip install sodapy pandas pyarrow tenacity requests

How to find the correct DATASET_ID
-----------------------------------
1. Go to https://www.datos.gov.co
2. Search "IPC DANE" or "Pobreza Monetaria Departamentos DANE"
3. Open the dataset page → click the "API" button
4. Copy the 4-character × 4-character identifier from the endpoint URL
   (e.g. ``/resource/er9f-6p48.json`` → DATASET_ID = "er9f-6p48")
"""

from __future__ import annotations

import logging
import traceback
from pathlib import Path
from typing import Optional

import pandas as pd
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout
from sodapy import Socrata
from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ─── Module logger ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-22s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Socrata / dataset configuration ─────────────────────────────────────────
SOCRATA_DOMAIN: str = "www.datos.gov.co"

# Column that stores the *year* value used for the >= 2018 SoQL filter.
# DANE encoding quirk: "año" often appears as "a_o" or "anio" — adjust here.
YEAR_COLUMN: str = "a_o"
MIN_YEAR: int = 2018

# Socrata timeout and retry settings
_SOCRATA_TIMEOUT_SECONDS: int = 60
_RETRY_ATTEMPTS: int = 4
_RETRY_WAIT_MIN_SECONDS: int = 4
_RETRY_WAIT_MAX_SECONDS: int = 60

# Default Parquet output path (overridable). In Airflow, mount /tmp as a volume.
DEFAULT_OUTPUT_PATH: str = "/tmp/raw_macro_data.parquet"

# Geographic column candidates searched in priority order for the data profile.
_GEO_COLUMN_CANDIDATES: tuple[str, ...] = (
    "departamento",
    "nombre_departamento",
    "municipio",
    "nombre_municipio",
    "dp",  # DIVIPOLA prefix code column
)

# Column-name keywords that hint the values are dates / periods.
_DATE_KEYWORDS: tuple[str, ...] = ("fecha", "periodo", "date", "mes_a_o", "mes")


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _build_where_clause(year_column: str, min_year: int) -> str:
    """Builds a SoQL WHERE clause to filter records at or after ``min_year``.

    Args:
        year_column: API column name that stores the year (e.g. ``"a_o"``).
        min_year: Lower bound, inclusive (e.g. ``2018``).

    Returns:
        SoQL-compatible WHERE string (e.g. ``"a_o >= '2018'"``).
    """
    return f"{year_column} >= '{min_year}'"


def _optimise_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Downcasts ``object`` columns to memory-efficient dtypes right after ingestion.

    Conversion strategy (applied per column, in order of priority):

    1. **Numeric** — if ≥ 90% of non-null values parse as numbers → ``float32``.
       (float64 wastes memory for macroeconomic indices; float32 gives ~7
       significant decimal digits, sufficient for IPC / poverty rates.)
    2. **Datetime** — if the column name contains a date-hint keyword AND the
       values parse cleanly as dates → ``datetime64[ns]``.
    3. **Category** — if the column's cardinality is < 50% of total rows
       (typical for ``departamento``, ``ciudad``, etc.) → ``category``.

    Args:
        df: Raw DataFrame with predominantly ``object`` dtypes from Socrata JSON.

    Returns:
        A new DataFrame with optimised dtypes. The original is not mutated.
    """
    df = df.copy()
    n_rows = len(df)

    for col in df.columns:
        series: pd.Series = df[col]

        if series.dtype != object:
            continue  # already typed; skip

        # ── 1. Numeric detection ──────────────────────────────────────────────
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null_original = series.notna()

        if non_null_original.any():
            numeric_hit_rate = numeric_series[non_null_original].notna().mean()
        else:
            numeric_hit_rate = 0.0

        if numeric_hit_rate >= 0.90:
            df[col] = numeric_series.astype("float32")
            logger.info("  dtype cast | %-38s object → float32", col)
            continue

        # ── 2. Datetime detection ─────────────────────────────────────────────
        col_lower = col.lower()
        if any(kw in col_lower for kw in _DATE_KEYWORDS):
            try:
                df[col] = pd.to_datetime(series, errors="raise", dayfirst=False)
                logger.info("  dtype cast | %-38s object → datetime64[ns]", col)
                continue
            except (ValueError, TypeError):
                pass  # not a parseable date — fall through

        # ── 3. Low-cardinality → category ────────────────────────────────────
        n_unique = series.nunique(dropna=True)
        if n_rows > 0 and (n_unique / n_rows) < 0.50:
            df[col] = series.astype("category")
            logger.info(
                "  dtype cast | %-38s object → category  (%d unique values)",
                col,
                n_unique,
            )

    return df


# ─── Paginated Socrata fetch (with tenacity retry) ────────────────────────────

@retry(
    retry=retry_if_exception_type((RequestsConnectionError, Timeout, HTTPError)),
    wait=wait_exponential(
        multiplier=1,
        min=_RETRY_WAIT_MIN_SECONDS,
        max=_RETRY_WAIT_MAX_SECONDS,
    ),
    stop=stop_after_attempt(_RETRY_ATTEMPTS),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _fetch_page(
    client: Socrata,
    dataset_id: str,
    where_clause: str,
    limit: int,
    offset: int,
) -> list[dict]:
    """Fetches a single paginated page from the Socrata API.

    Decorated with ``@retry`` (exponential back-off) to handle transient
    network failures and HTTP 5xx errors without crashing the pipeline.

    Args:
        client: Authenticated sodapy ``Socrata`` context-manager client.
        dataset_id: Socrata 4x4 dataset identifier.
        where_clause: SoQL WHERE filter string.
        limit: Maximum records to return in this page.
        offset: Zero-based record index to start from.

    Returns:
        List of record dicts. An empty list signals the end of pagination.

    Raises:
        ConnectionError: After all retry attempts are exhausted (network issue).
        Timeout: After all retry attempts are exhausted (API unresponsive).
        HTTPError: After all retry attempts are exhausted (API HTTP error).
    """
    return client.get(
        dataset_id,
        where=where_clause,
        limit=limit,
        offset=offset,
        order=":id",  # deterministic ordering is required for correct pagination
    )


# ─── Public interface ─────────────────────────────────────────────────────────

def extract_macroeconomic_data(
    dataset_id: str,
    app_token: str,
    limit: int = 50_000,
    output_path: str = DEFAULT_OUTPUT_PATH,
    year_column: str = YEAR_COLUMN,
    min_year: int = MIN_YEAR,
) -> str:
    """Extracts macroeconomic data from datos.gov.co and persists it as Parquet.

    Handles API pagination automatically (loops in ``limit``-sized pages until
    an empty response is received). Dtypes are optimised in-memory before
    serialisation to reduce file size.

    Designed to run as a standalone script **or** as an Airflow
    ``PythonOperator`` / ``@task`` function — it returns a file path string
    suitable for passing via XCom, never a raw DataFrame.  Receiving
    ``dataset_id`` as an explicit argument (rather than reading a module
    constant) allows the same function to be called multiple times within a
    single DAG for different datasets (e.g. IPC and Pobreza Monetaria) without
    touching the source code.

    Args:
        dataset_id: Socrata 4x4 dataset identifier. Find it at datos.gov.co:
            open the dataset page → click "API" → copy the identifier from the
            endpoint URL (e.g. ``/resource/er9f-6p48.json`` → ``"er9f-6p48"``).
        app_token: Socrata application token. Obtain one (free) at
            https://data.socrata.com/profile/edit/developer_settings.
            Pass an empty string to run unauthenticated (heavy throttling).
        limit: Records per API page. Socrata enforces a hard maximum of 50,000.
        output_path: Absolute path for the output ``.parquet`` file.
            Parent directories are created automatically.
        year_column: Name of the year column used to build the SoQL filter.
            Common values: ``"a_o"``, ``"anio"``, ``"year"``.
        min_year: Only records with ``year_column >= min_year`` are fetched.

    Returns:
        Absolute path to the written Parquet file (pass this string as the
        Airflow XCom value to downstream tasks).

    Raises:
        RuntimeError: If the API returns zero records after all pages are
            fetched (indicates wrong ``dataset_id`` or ``year_column``).
        ConnectionError: If the Socrata API is unreachable after all retries.
        Exception: Any unrecoverable error is logged at ERROR level with a
            full stack trace before being re-raised.
    """
    logger.info(
        "Extraction started | domain=%s | dataset=%s | filter: %s >= %d",
        SOCRATA_DOMAIN,
        dataset_id,
        year_column,
        min_year,
    )

    where_clause = _build_where_clause(year_column, min_year)
    all_records: list[dict] = []
    offset: int = 0

    try:
        with Socrata(
            SOCRATA_DOMAIN,
            app_token=app_token or None,  # sodapy treats empty string as unauthenticated
            timeout=_SOCRATA_TIMEOUT_SECONDS,
        ) as client:
            while True:
                logger.info(
                    "Requesting page | offset=%d | limit=%d | where='%s'",
                    offset,
                    limit,
                    where_clause,
                )

                page = _fetch_page(
                    client,
                    dataset_id=dataset_id,
                    where_clause=where_clause,
                    limit=limit,
                    offset=offset,
                )

                if not page:
                    logger.info("Empty page — pagination complete.")
                    break

                all_records.extend(page)
                logger.info(
                    "Page received | records_in_page=%d | cumulative_total=%d",
                    len(page),
                    len(all_records),
                )

                if len(page) < limit:
                    # Partial page → last available page
                    logger.info("Partial page (%d < %d) — last page reached.", len(page), limit)
                    break

                offset += limit

        if not all_records:
            raise RuntimeError(
                f"Socrata API returned 0 records for dataset='{dataset_id}' "
                f"with filter '{where_clause}'. "
                "Verify the dataset ID and the year_column name on datos.gov.co."
            )

        logger.info("Building DataFrame from %d total records…", len(all_records))
        df = pd.DataFrame.from_records(all_records)
        logger.info(
            "Raw shape: %d rows × %d cols | memory: %.2f MB",
            *df.shape,
            df.memory_usage(deep=True).sum() / 1_048_576,
        )

        logger.info("Optimising dtypes…")
        df = _optimise_dtypes(df)
        logger.info(
            "Optimised shape: %d rows × %d cols | memory: %.2f MB",
            *df.shape,
            df.memory_usage(deep=True).sum() / 1_048_576,
        )

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_file, index=False, engine="pyarrow", compression="snappy")

        file_size_mb = output_file.stat().st_size / 1_048_576
        logger.info(
            "Parquet written successfully | path=%s | size=%.2f MB | rows=%d | cols=%d",
            output_file,
            file_size_mb,
            len(df),
            len(df.columns),
        )
        return str(output_file)

    except RetryError as exc:
        logger.error(
            "All %d retry attempts exhausted contacting Socrata API.\n%s",
            _RETRY_ATTEMPTS,
            traceback.format_exc(),
        )
        raise ConnectionError("Socrata API unreachable after retries.") from exc

    except Exception:
        logger.error(
            "Unrecoverable error during macroeconomic data extraction.\n%s",
            traceback.format_exc(),
        )
        raise


def profile_api_data(parquet_path: str) -> None:
    """Reads a raw Parquet file and emits a data quality profile via the logger.

    Produces two audit sections critical for the subsequent fuzzy-matching
    step against ``dim_geography``:

    1. **Null audit** — percentage of null values per column, flagging any
       column above 5% with a warning marker. High nulls in key columns
       (e.g. ``departamento``) will break the ``LEFT JOIN`` in the Transform.

    2. **Geographic key audit** — lists every unique value found in the first
       recognised geographic column (``departamento``, ``municipio``, etc.).
       This output must be inspected before running TheFuzz / RapidFuzz
       similarity matching against the ``dim_geography`` lookup table, since
       DANE datasets often contain dirty variants like ``"VALLE"`` vs
       ``"VALLE DEL CAUCA"`` or ``"BOGOTÁ D.C."`` vs ``"BOGOTA"``.

    Args:
        parquet_path: Absolute path to the ``.parquet`` file produced by
            :func:`extract_macroeconomic_data`.

    Returns:
        None. All output is emitted via ``logging.INFO`` / ``logging.WARNING``.

    Raises:
        FileNotFoundError: If ``parquet_path`` does not exist on disk.
        ValueError: If no geographic column matching the known candidates is
            found in the DataFrame (update ``_GEO_COLUMN_CANDIDATES`` if the
            DANE dataset uses a non-standard column name).
    """
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found at: {path}")

    df = pd.read_parquet(path, engine="pyarrow")
    n_rows, n_cols = df.shape

    separator = "=" * 70
    logger.info(separator)
    logger.info("DATA PROFILE  —  %s", path.name)
    logger.info(separator)
    logger.info("Shape   : %d rows × %d columns", n_rows, n_cols)
    logger.info(
        "Memory  : %.2f MB  (post-dtype optimisation)",
        df.memory_usage(deep=True).sum() / 1_048_576,
    )

    # ── Section 1: Null audit ─────────────────────────────────────────────────
    logger.info("-" * 70)
    logger.info("NULL PERCENTAGE BY COLUMN")
    logger.info("-" * 70)

    null_pct: pd.Series = (df.isna().sum() / n_rows * 100).sort_values(ascending=False)
    for col, pct in null_pct.items():
        warning_flag = "  ← WARNING: >5%% nulls" if pct > 5.0 else ""
        logger.info("  %-40s %6.2f%%%s", col, pct, warning_flag)

    # ── Section 2: Geographic key audit ──────────────────────────────────────
    logger.info("-" * 70)
    logger.info("GEOGRAPHIC KEY AUDIT  (inspect before fuzzy-match vs dim_geography)")
    logger.info("-" * 70)

    # Case-insensitive column lookup
    cols_lower_map: dict[str, str] = {c.lower(): c for c in df.columns}
    geo_col: Optional[str] = None

    for candidate in _GEO_COLUMN_CANDIDATES:
        if candidate in cols_lower_map:
            geo_col = cols_lower_map[candidate]
            break

    if geo_col is None:
        logger.warning(
            "No geographic column found. Candidates searched: %s. "
            "Columns available: %s",
            list(_GEO_COLUMN_CANDIDATES),
            list(df.columns),
        )
        raise ValueError(
            f"Could not locate a geographic column in the dataset. "
            f"Candidates searched (in priority order): {list(_GEO_COLUMN_CANDIDATES)}. "
            f"Columns present: {list(df.columns)}. "
            "Add the correct column name to _GEO_COLUMN_CANDIDATES."
        )

    unique_geo: list[str] = sorted(df[geo_col].dropna().astype(str).unique())

    logger.info("Geographic column : '%s'", geo_col)
    logger.info("Unique values     : %d", len(unique_geo))
    logger.info(
        "Null rows in col  : %d (%.2f%%)",
        df[geo_col].isna().sum(),
        df[geo_col].isna().sum() / n_rows * 100,
    )
    logger.info("Values (copy this list into your fuzzy-matcher configuration):")

    for idx, val in enumerate(unique_geo, start=1):
        logger.info("  [%02d] %s", idx, val)

    logger.info(separator)
    logger.info("Profile complete. Review geographic values above before transform.")
    logger.info(separator)


# ─── Local test harness ────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os

    _TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")

    if not _TOKEN:
        logger.warning(
            "SOCRATA_APP_TOKEN environment variable is not set. "
            "Running unauthenticated — expect aggressive rate limiting from Socrata. "
            "Set the variable and retry for production use."
        )

    # Example IDs (verify current values at datos.gov.co):
    #   IPC Base Dic 2018  → "er9f-6p48"
    #   Pobreza Monetaria  → "hfcb-tij6"
    _DATASET_ID = os.getenv("SOCRATA_DATASET_ID", "er9f-6p48")

    _parquet_path = extract_macroeconomic_data(dataset_id=_DATASET_ID, app_token=_TOKEN)
    profile_api_data(_parquet_path)
