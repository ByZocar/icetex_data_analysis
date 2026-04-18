"""
validate_data.py
================
Phase 2 — ETL Pipeline: Airflow Data Quality Gate (Task 3)

Loads the File-Backed Great Expectations Data Context from ``./gx/`` and
runs the ``icetex_macro_checkpoint`` against the merged Parquet produced by
``transform_and_merge_data``.  Acts as a hard gate: a failed suite halts the
DAG before any record reaches PostgreSQL.

Airflow task topology::

    [Task 2] transform_and_merge_data
         │
         │  XCom: /tmp/merged_data.parquet
         ▼
    [Task 3] run_data_quality_checks      ← this module
         │
         │  XCom: /tmp/merged_data.parquet (pass-through if valid)
         ▼
    [Task 4] load_to_postgres

DAG wiring example::

    from src.validate_data import run_data_quality_checks

    validate_task = PythonOperator(
        task_id="run_gx_validation",
        python_callable=run_data_quality_checks,
        op_args=["{{ ti.xcom_pull(task_ids='transform_and_merge_data') }}"],
    )

Failure behaviour
-----------------
* **Inside Airflow**: raises ``AirflowFailException`` — marks the task as
  FAILED with no automatic retry (data errors are not transient; retrying
  would produce the same result).
* **Standalone / tests**: raises ``ValueError`` with a human-readable summary
  of every failed expectation.

Data Docs
---------
After each run (pass or fail) the checkpoint's ``UpdateDataDocsAction``
regenerates the HTML report at::

    gx/uncommitted/data_docs/local_site/index.html

Open it in a browser to inspect per-column pass rates and expectation details.
"""

from __future__ import annotations

import logging
import traceback
from pathlib import Path
from typing import Any

import great_expectations as gx
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult

# ── Airflow exception — imported conditionally so the module works standalone ─
try:
    from airflow.exceptions import AirflowFailException as _PipelineFailure
    _RUNNING_IN_AIRFLOW = True
except ImportError:
    _PipelineFailure = ValueError  # type: ignore[assignment, misc]
    _RUNNING_IN_AIRFLOW = False

# ─── Logger ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-26s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Paths ────────────────────────────────────────────────────────────────────
_SRC_DIR = Path(__file__).resolve().parent   # …/Icetex/src/
_PROJECT_ROOT = _SRC_DIR.parent             # …/Icetex/  ← passed to get_context
GX_ROOT = _PROJECT_ROOT / "gx"             # …/Icetex/gx/ ← where GX writes files

# ─── GX object identifiers (must match setup_gx.py) ──────────────────────────
DATASOURCE_NAME: str = "parquet_datasource"
ASSET_NAME: str = "merged_parquet_asset"
CHECKPOINT_NAME: str = "icetex_macro_checkpoint"
SUITE_NAME: str = "icetex_macro_suite"


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _load_context() -> gx.DataContext:
    """Loads the File-Backed Data Context from ``GX_ROOT``.

    Returns:
        Active ``FileDataContext`` instance.

    Raises:
        FileNotFoundError: If ``GX_ROOT`` does not exist.  This means
            ``setup_gx.py`` has not been run yet.
    """
    if not GX_ROOT.exists():
        raise FileNotFoundError(
            f"GX context directory not found: {GX_ROOT}.\n"
            "Run 'python src/setup_gx.py' from the project root first."
        )
    return gx.get_context(
        mode="file",
        project_root_dir=str(_PROJECT_ROOT),  # parent of gx/, not gx/ itself
    )


def _get_batch_request(
    context: gx.DataContext,
    parquet_file: Path,
) -> Any:
    """Reads the Parquet file and returns a batch request backed by its DataFrame.

    Uses ``PandasDatasource`` (in-memory) — the same datasource registered by
    ``setup_gx.py`` — to avoid GX's ``test_connection()`` filesystem check.
    The DataFrame is read here with pandas and attached to the batch request so
    GX can validate it against the stored expectation suite.

    Args:
        context: Active ``FileDataContext``.
        parquet_file: Resolved absolute path to the validated Parquet.

    Returns:
        A ``BatchRequest`` with the Parquet DataFrame attached.
    """
    import pandas as pd

    logger.info("Reading Parquet for validation: %s", parquet_file)
    df = pd.read_parquet(parquet_file, engine="pyarrow")
    logger.info("Parquet loaded | shape: %d rows × %d cols", *df.shape)

    datasource = context.sources.add_or_update_pandas(name=DATASOURCE_NAME)

    _registered = {a.name for a in datasource.assets}
    if ASSET_NAME not in _registered:
        datasource.add_dataframe_asset(name=ASSET_NAME)

    asset = datasource.get_asset(ASSET_NAME)
    batch_request = asset.build_batch_request(dataframe=df)
    logger.info("Batch request built | asset='%s' | rows=%d", ASSET_NAME, len(df))
    return batch_request


def _extract_failures(result: CheckpointResult) -> list[dict[str, Any]]:
    """Parses a ``CheckpointResult`` and returns a list of failed expectations.

    Args:
        result: The ``CheckpointResult`` returned by ``context.run_checkpoint``.

    Returns:
        List of dicts, each describing one failed expectation::

            {
                "type": "expect_column_values_to_not_be_null",
                "column": "vigencia",
                "kwargs": {...},
                "observed": {...},
            }
    """
    failures: list[dict[str, Any]] = []

    for run_result in result.run_results.values():
        validation_result = run_result.get("validation_result", {})
        for exp_result in validation_result.get("results", []):
            if not exp_result.get("success", True):
                config = exp_result.get("expectation_config", {})
                failures.append(
                    {
                        "type": config.get("expectation_type", "unknown"),
                        "column": config.get("kwargs", {}).get("column", "TABLE-LEVEL"),
                        "kwargs": config.get("kwargs", {}),
                        "observed": exp_result.get("result", {}),
                    }
                )
    return failures


def _format_failure_report(failures: list[dict[str, Any]], parquet_name: str) -> str:
    """Formats the failure list into a human-readable string for log and exception.

    Args:
        failures: Output of ``_extract_failures()``.
        parquet_name: Filename used in the report header.

    Returns:
        Multi-line string suitable for logging and exception messages.
    """
    lines = [
        f"Great Expectations FAILED for '{parquet_name}'.",
        f"{len(failures)} expectation(s) did not pass:\n",
    ]
    for i, f in enumerate(failures, start=1):
        lines.append(
            f"  [{i}] {f['type']}"
            f" | column={f['column']}"
            f" | observed={f['observed']}"
        )
    lines.append(
        "\nReview Data Docs for per-column pass rates and sample failing values: "
        f"{GX_ROOT / 'uncommitted' / 'data_docs' / 'local_site' / 'index.html'}"
    )
    return "\n".join(lines)


# ─── Public interface ─────────────────────────────────────────────────────────

def run_data_quality_checks(parquet_path: str) -> str:
    """Validates the merged Parquet against the ``icetex_macro_suite`` rules.

    Loads the persisted GX File-Backed Data Context, builds a runtime batch
    request for ``parquet_path``, and executes ``icetex_macro_checkpoint``.
    The checkpoint's action list stores the result and rebuilds Data Docs
    automatically.

    Args:
        parquet_path: Absolute path to the merged ``.parquet`` file produced
            by ``transform_and_merge_data``.  In Airflow, receive this via
            ``ti.xcom_pull(task_ids='transform_and_merge_data')``.

    Returns:
        ``parquet_path`` unchanged — safe to push as XCom to Task 4
        (``load_to_postgres``) so the load task knows where to read from.

    Raises:
        FileNotFoundError: If ``GX_ROOT`` is missing (setup not run) or if
            ``parquet_path`` does not exist on disk.
        AirflowFailException: (inside Airflow) When one or more expectations
            fail.  The task is marked FAILED with no retry.
        ValueError: (standalone / tests) Same failure condition, no Airflow
            dependency required.
        Exception: Any unexpected GX or I/O error is logged with full stack
            trace before being re-raised.
    """
    logger.info("=" * 68)
    logger.info("DATA QUALITY GATE — GX Checkpoint Execution")
    logger.info("=" * 68)
    logger.info("Input Parquet  : %s", parquet_path)
    logger.info("GX Context root: %s", GX_ROOT)
    logger.info(
        "Running in Airflow: %s",
        _RUNNING_IN_AIRFLOW,
    )

    parquet_file = Path(parquet_path).resolve()
    if not parquet_file.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {parquet_file}. "
            "Verify that 'transform_and_merge_data' completed successfully."
        )

    try:
        # ── 1. Load context ───────────────────────────────────────────────────
        context = _load_context()
        logger.info("Context loaded (type=%s).", type(context).__name__)

        # ── 2. Build runtime batch request ────────────────────────────────────
        batch_request = _get_batch_request(context, parquet_file)

        # ── 3. Run checkpoint with runtime validations ────────────────────────
        logger.info("Running checkpoint '%s'…", CHECKPOINT_NAME)
        result: CheckpointResult = context.run_checkpoint(
            checkpoint_name=CHECKPOINT_NAME,
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": SUITE_NAME,
                }
            ],
        )

        logger.info(
            "Checkpoint finished | success=%s | run_id=%s",
            result.success,
            result.run_id,
        )

        # ── 4. Handle result ──────────────────────────────────────────────────
        if not result.success:
            failures = _extract_failures(result)
            report = _format_failure_report(failures, parquet_file.name)

            logger.error(report)
            raise _PipelineFailure(report)

        logger.info(
            "All expectations PASSED for '%s'. Proceeding to load phase.",
            parquet_file.name,
        )
        logger.info("=" * 68)
        return str(parquet_path)

    except (_PipelineFailure, FileNotFoundError):
        # Re-raise these directly — they already carry a clear message.
        raise

    except Exception:
        logger.error(
            "Unexpected error during GX validation.\n%s",
            traceback.format_exc(),
        )
        raise


# ─── Local smoke-test ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    import sys

    _PARQUET = os.getenv("MERGED_PARQUET_PATH", "/tmp/merged_data.parquet")

    logger.info("Standalone smoke-test | parquet=%s", _PARQUET)
    try:
        validated_path = run_data_quality_checks(_PARQUET)
        logger.info("Smoke-test PASSED. Output path: %s", validated_path)
    except (ValueError, FileNotFoundError) as exc:
        logger.error("Smoke-test FAILED: %s", exc)
        sys.exit(1)
