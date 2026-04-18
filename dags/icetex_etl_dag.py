"""
icetex_etl_dag.py
=================
ICETEX ETL Pipeline — Apache Airflow DAG (TaskFlow API)

Orchestrates the full end-to-end ETL that loads the ICETEX credit cohorte
CSV and DANE macroeconomic API data into the ``icetex_ods_dw`` PostgreSQL
star schema.  All inter-task communication passes file-path strings via
Airflow XCom; no DataFrame ever transits the message broker.

Pipeline topology (parallelism at extraction, then linear)::

    ┌─────────────────────┐   ┌──────────────────────────┐
    │  Task 1a            │   │  Task 1b                 │
    │  extract_icetex_csv │   │  extract_macro_api       │
    │  → /tmp/icetex.pq   │   │  → /tmp/raw_macro.pq     │
    └────────┬────────────┘   └────────────┬─────────────┘
             │                             │
             └──────────┬──────────────────┘
                        ▼
              ┌──────────────────────┐
              │  Task 2              │
              │  transform_and_merge │
              │  → /tmp/merged.pq    │
              └──────────┬───────────┘
                         ▼
              ┌──────────────────────┐
              │  Task 3              │
              │  run_quality_checks  │  ← hard gate: fails DAG on bad data
              │  → /tmp/merged.pq    │    (pass-through path on success)
              └──────────┬───────────┘
                         ▼
              ┌──────────────────────┐
              │  Task 4              │
              │  load_to_postgres    │
              └──────────────────────┘

Schedule
--------
Runs monthly (``@monthly``).  ``catchup=False`` prevents Airflow from
backfilling historical DAG runs on first activation — the pipeline is
designed for incremental monthly loads driven by ICETEX data releases,
not for automated history reconstruction.

Environment variables (set in ``docker-compose.yml`` → ``.env``)
----------------------------------------------------------------
``ICETEX_DW_URI``
    SQLAlchemy connection string for the data warehouse
    (``postgresql+psycopg2://icetex:<pw>@postgres-dw:5432/icetex_ods_dw``).
``SOCRATA_APP_TOKEN``
    Optional API token for datos.gov.co rate-limit relief.
``SOCRATA_DATASET_ID``
    Socrata resource ID for the primary macro dataset (default ``er9f-6p48``).
``ICETEX_CSV_PATH``
    Absolute path to the raw ICETEX credit CSV inside the container
    (default ``/opt/airflow/data/raw/Créditos_Otorgados._20260304.csv``).

Idempotency
-----------
Re-running the DAG for a month that has already been loaded is safe:

* Dimensions: ``INSERT … ON CONFLICT DO NOTHING`` — existing rows are skipped.
* Facts: watermark DELETE (by ``vigencia``) + re-INSERT — the previous month's
  rows are replaced atomically with the new batch.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

# ─── Logger ───────────────────────────────────────────────────────────────────
log = logging.getLogger(__name__)

# ─── Default arguments applied to every task ─────────────────────────────────
_DEFAULT_ARGS = {
    "owner": "icetex-etl",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── Staging paths (inside the shared /tmp volume) ───────────────────────────
_ICETEX_PARQUET: str = "/tmp/icetex_raw.parquet"
_MACRO_PARQUET: str = "/tmp/raw_macro_data.parquet"
_MERGED_PARQUET: str = "/tmp/merged_data.parquet"

# ─── Source CSV path (mounted at /opt/airflow/data via docker-compose) ────────
_DEFAULT_CSV_PATH: str = (
    "/opt/airflow/data/raw/Créditos_Otorgados._20260304.csv"
)


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────

@dag(
    dag_id="icetex_etl_pipeline",
    description=(
        "End-to-end ETL: ICETEX credit cohorte + DANE macro API → "
        "PostgreSQL star schema (icetex_ods_dw)"
    ),
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["icetex", "etl", "dane", "crowding-out"],
)
def icetex_etl_pipeline() -> None:
    """ICETEX crowding-out effect ETL pipeline."""

    # ── Task 1a: Extract ICETEX CSV ──────────────────────────────────────────

    @task(task_id="extract_icetex_csv")
    def extract_icetex_csv() -> str:
        """Reads the ICETEX credit CSV, cleans it, and writes a Parquet staging file.

        Applies ``transform.clean_and_standardize`` so that downstream tasks
        receive a ready-to-merge DataFrame with normalised text, corrected
        ``VIGENCIA`` dtype (int), and ``SECTOR IES`` nulls filled.

        Returns:
            Absolute path to the written Parquet file (``_ICETEX_PARQUET``).

        Raises:
            FileNotFoundError: If the source CSV is not mounted at the
                expected path.  Check the ``data/`` volume in
                ``docker-compose.yml``.
            AirflowFailException: On any unexpected parse or I/O error.
        """
        from src.extract import extract_csv
        from src.transform import clean_and_standardize

        csv_path: str = os.environ.get("ICETEX_CSV_PATH", _DEFAULT_CSV_PATH)
        log.info("Task 1a | Reading ICETEX CSV: %s", csv_path)

        try:
            raw_df: pd.DataFrame = extract_csv(csv_path)
            log.info(
                "Task 1a | Raw shape: %d rows × %d cols",
                raw_df.shape[0],
                raw_df.shape[1],
            )

            clean_df: pd.DataFrame = clean_and_standardize(raw_df)
            log.info(
                "Task 1a | Clean shape: %d rows × %d cols",
                clean_df.shape[0],
                clean_df.shape[1],
            )

            clean_df.to_parquet(_ICETEX_PARQUET, index=False, engine="pyarrow")
            log.info("Task 1a | Parquet written → %s", _ICETEX_PARQUET)
            return _ICETEX_PARQUET

        except FileNotFoundError:
            raise
        except Exception as exc:
            raise AirflowFailException(
                f"Task 1a failed — could not extract/clean ICETEX CSV: {exc}"
            ) from exc

    # ── Task 1b: Extract macroeconomic data via Socrata API ──────────────────

    @task(task_id="extract_macro_api")
    def extract_macro_api() -> str:
        """Pulls macroeconomic data from datos.gov.co and writes a Parquet file.

        Delegates entirely to ``src.extract_api.extract_macroeconomic_data``,
        which handles Socrata pagination, dtype optimisation, and retry logic
        (exponential backoff, up to 4 attempts).

        Environment variables consumed:
            ``SOCRATA_APP_TOKEN`` — API token (empty string if absent).
            ``SOCRATA_DATASET_ID`` — Socrata resource ID (default ``er9f-6p48``).

        Returns:
            Absolute path to the written Parquet file (``_MACRO_PARQUET``).

        Raises:
            AirflowFailException: If all Socrata retry attempts are exhausted
                or the API returns an unrecoverable error.
        """
        from src.extract_api import extract_macroeconomic_data

        dataset_id: str = os.environ.get("SOCRATA_DATASET_ID", "er9f-6p48")
        app_token: str = os.environ.get("SOCRATA_APP_TOKEN", "")

        log.info(
            "Task 1b | Extracting Socrata dataset '%s' (token present: %s)",
            dataset_id,
            bool(app_token),
        )

        try:
            parquet_path: str = extract_macroeconomic_data(
                dataset_id=dataset_id,
                app_token=app_token,
                output_path=_MACRO_PARQUET,
            )
            log.info("Task 1b | Macro Parquet written → %s", parquet_path)
            return parquet_path

        except Exception as exc:
            raise AirflowFailException(
                f"Task 1b failed — Socrata extraction error for dataset "
                f"'{dataset_id}': {exc}"
            ) from exc

    # ── Task 2: Fuzzy-match geography and LEFT JOIN ───────────────────────────

    @task(task_id="transform_and_merge")
    def transform_and_merge(icetex_parquet: str, macro_parquet: str) -> str:
        """Fuzzy-matches department names and LEFT JOINs ICETEX with macro data.

        Reads the ICETEX Parquet written by Task 1a back into a DataFrame
        (required because ``transform_and_merge_data`` expects a DataFrame as
        its first argument, not a file path), then delegates the full 8-step
        pipeline to ``src.transform_and_merge.transform_and_merge_data``.

        Args:
            icetex_parquet: XCom path from Task 1a (``_ICETEX_PARQUET``).
            macro_parquet:  XCom path from Task 1b (``_MACRO_PARQUET``).

        Returns:
            Absolute path to the merged Parquet file (``_MERGED_PARQUET``).

        Raises:
            AirflowFailException: On fuzzy-match failure or I/O errors.
        """
        from src.transform_and_merge import transform_and_merge_data

        log.info(
            "Task 2 | Merging ICETEX='%s' + macro='%s'",
            icetex_parquet,
            macro_parquet,
        )

        try:
            icetex_df: pd.DataFrame = pd.read_parquet(
                icetex_parquet, engine="pyarrow"
            )
            log.info(
                "Task 2 | ICETEX Parquet loaded | shape: %d × %d",
                icetex_df.shape[0],
                icetex_df.shape[1],
            )

            merged_path: str = transform_and_merge_data(
                icetex_df=icetex_df,
                api_parquet_path=macro_parquet,
                output_path=_MERGED_PARQUET,
            )
            log.info("Task 2 | Merged Parquet written → %s", merged_path)
            return merged_path

        except Exception as exc:
            raise AirflowFailException(
                f"Task 2 failed — transform/merge error: {exc}"
            ) from exc

    # ── Task 3: Great Expectations data quality gate ──────────────────────────

    @task(task_id="run_quality_checks")
    def run_quality_checks(merged_parquet: str) -> str:
        """Runs the GX ``icetex_macro_suite`` checkpoint against the merged Parquet.

        Acts as a hard gate: if any expectation fails, the task is marked
        FAILED and no retry is attempted (data errors are not transient).
        The checkpoint's ``UpdateDataDocsAction`` regenerates the HTML report
        regardless of pass/fail outcome.

        Args:
            merged_parquet: XCom path from Task 2 (``_MERGED_PARQUET``).

        Returns:
            ``merged_parquet`` unchanged — passed through to Task 4 via XCom.

        Raises:
            AirflowFailException: When one or more GX expectations fail.
                Inspect Data Docs at
                ``gx/uncommitted/data_docs/local_site/index.html``.
            FileNotFoundError: If ``gx/`` context directory is missing.
                Run ``python src/setup_gx.py`` from the project root first.
        """
        from src.validate_data import run_data_quality_checks

        log.info("Task 3 | Running GX checkpoint on: %s", merged_parquet)

        # ``run_data_quality_checks`` already raises AirflowFailException
        # on validation failure — no additional wrapping needed here.
        validated_path: str = run_data_quality_checks(merged_parquet)

        log.info("Task 3 | GX validation PASSED. Path: %s", validated_path)
        return validated_path

    # ── Task 4: Load to PostgreSQL star schema ────────────────────────────────

    @task(task_id="load_to_postgres")
    def load_to_postgres(validated_parquet: str) -> None:
        """Loads the validated Parquet into the ``icetex_ods_dw`` star schema.

        Dimensions are loaded with ``INSERT … ON CONFLICT DO NOTHING``
        (idempotent; new rows get fresh SERIAL SKs, existing rows are skipped).
        Facts are loaded with a watermark DELETE + chunked INSERT strategy
        (replaces rows for the current batch's ``vigencia`` years atomically).

        All four tasks run inside a single ``engine.begin()`` transaction.
        A rollback on any error leaves the DW in its pre-run state.

        Args:
            validated_parquet: XCom path from Task 3 (``_MERGED_PARQUET``).

        Raises:
            AirflowFailException: If ``ICETEX_DW_URI`` is not set, or on any
                database error during the load.
        """
        from src.load_postgres import load_to_datawarehouse

        db_uri: str | None = os.environ.get("ICETEX_DW_URI")
        if not db_uri:
            raise AirflowFailException(
                "Task 4 failed — environment variable 'ICETEX_DW_URI' is not set. "
                "Verify the docker-compose.yml environment block and .env file."
            )

        log.info(
            "Task 4 | Loading '%s' → PostgreSQL DW",
            validated_parquet,
        )

        try:
            load_to_datawarehouse(
                parquet_path=validated_parquet,
                db_uri=db_uri,
            )
            log.info("Task 4 | Load complete. Star schema updated.")

        except Exception as exc:
            raise AirflowFailException(
                f"Task 4 failed — PostgreSQL load error: {exc}"
            ) from exc

    # ── Wire the task graph ───────────────────────────────────────────────────
    # Tasks 1a and 1b run in parallel (no dependency between them).
    # Task 2 fans in: it receives both XCom outputs as named arguments.
    # Tasks 3 and 4 run sequentially after Task 2.

    icetex_path: str = extract_icetex_csv()
    macro_path: str = extract_macro_api()

    merged_path: str = transform_and_merge(
        icetex_parquet=icetex_path,
        macro_parquet=macro_path,
    )

    validated_path: str = run_quality_checks(merged_parquet=merged_path)

    load_to_postgres(validated_parquet=validated_path)


# ─── Register the DAG with Airflow ───────────────────────────────────────────
# Calling the decorated function returns a DAG object. Assigning it to a
# module-level variable ensures the Airflow scheduler can discover it.
icetex_etl_pipeline_dag = icetex_etl_pipeline()
