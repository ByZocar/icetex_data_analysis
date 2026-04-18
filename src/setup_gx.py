"""
setup_gx.py
===========
Phase 2 — Great Expectations Bootstrap (File-Backed Data Context)

Creates and persists the full GX infrastructure in ``./gx/`` so that every
Airflow worker (and every developer) works against an identical, version-
controlled configuration.  Run once before the first pipeline execution, or
re-run any time the suite rules change (fully idempotent).

Execution::

    # From the project root
    python src/setup_gx.py

What this script creates
-------------------------
``gx/``
├── great_expectations.yml      ← context configuration
├── expectations/
│   └── icetex_macro_suite.json ← the suite defined below  ← commit this
├── checkpoints/
│   └── icetex_macro_checkpoint.yml                        ← commit this
└── uncommitted/                                           ← gitignored by GX
    ├── data_docs/local_site/   ← HTML report (generated at validation time)
    └── validations/            ← per-run JSON results

Git strategy
-------------
Commit the entire ``gx/`` directory EXCEPT ``gx/uncommitted/``.  GX
automatically creates a ``gx/.gitignore`` that excludes ``uncommitted/``.

Column name contract
---------------------
The expectations below reference the **star-schema column names** produced by
``transform.run_transformation()`` (snake_case).  The ``merged_data.parquet``
fed to the GX task must therefore be the FULLY TRANSFORMED flat file — i.e.,
the output of Task 2 after column renaming — not the raw merged output with
UPPERCASE ICETEX names.

Mapping (ICETEX raw → validated snake_case):

    VIGENCIA                              → vigencia
    CÓDIGO DEDEPARTAMENTO DE ORIGEN       → codigo_departamento
    SECTOR IES                            → sector_ies
    ESTRATO SOCIOECONÓMICO                → estrato_socioeconomico
    NÚMERO DE NUEVOS BENEFICIARIOS DE CRÉDITO → total_nuevos_beneficiarios
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration

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

# ─── GX object identifiers ───────────────────────────────────────────────────
DATASOURCE_NAME: str = "parquet_datasource"
ASSET_NAME: str = "merged_parquet_asset"
SUITE_NAME: str = "icetex_macro_suite"
CHECKPOINT_NAME: str = "icetex_macro_checkpoint"

# ─── Column name constants ────────────────────────────────────────────────────
# These must match the ACTUAL column names in the merged Parquet file.
# The transform step preserves the original ICETEX column names (UPPERCASE).
COL_YEAR: str = "VIGENCIA"
COL_DEPT_CODE: str = "CÓDIGO DEDEPARTAMENTO DE ORIGEN"
COL_SECTOR: str = "SECTOR IES"
COL_ESTRATO: str = "ESTRATO SOCIOECONÓMICO"
COL_BENEFICIARIES: str = "NÚMERO DE NUEVOS BENEFICIARIOS DE CRÉDITO"

# ─── Domain value sets ────────────────────────────────────────────────────────
SECTOR_IES_ALLOWED: list[str] = ["OFICIAL", "PRIVADA", "PRIVADO", "NO CLASIFICADO"]
# Strata 1–3 are the ICETEX target population; 4–6 may appear but are out of
# scope for the "crowding-out" analysis.  Use mostly=0.95 to tolerate the
# ~5 % of records from higher strata that appear in the raw data.
ESTRATO_ALLOWED: list[int] = [1, 2, 3, 4, 5, 6]

# Volume bounds derived from the ICETEX dataset size (100k–120k rows).
# A count below MIN triggers the JOIN-failure alarm; above MAX flags runaway
# cross-joins before they hit PostgreSQL.
ROW_COUNT_MIN: int = 50_000
ROW_COUNT_MAX: int = 150_000


# ─── Step 1: Context ─────────────────────────────────────────────────────────

def initialise_context() -> gx.DataContext:
    """Creates or loads a File-Backed Data Context at ``GX_ROOT``.

    Uses ``gx.get_context(mode="file")`` (GX ≥ 0.15 fluent API).  The call is
    idempotent: if ``gx/great_expectations.yml`` already exists the existing
    context is returned unchanged.

    Returns:
        A ``FileDataContext`` instance pointing at ``GX_ROOT``.

    Raises:
        RuntimeError: If GX cannot initialise the context directory.
    """
    GX_ROOT.mkdir(parents=True, exist_ok=True)
    logger.info("Initialising FileDataContext at: %s", GX_ROOT)

    # project_root_dir must be the PARENT of the gx/ folder.
    # GX appends 'gx/' internally → passing GX_ROOT would create gx/gx/.
    context = gx.get_context(
        mode="file",
        project_root_dir=str(_PROJECT_ROOT),
    )
    logger.info("Context type: %s | GX version: %s", type(context).__name__, gx.__version__)
    return context


# ─── Step 2: Datasource ──────────────────────────────────────────────────────

def configure_datasource(context: gx.DataContext) -> None:
    """Registers an in-memory ``PandasDatasource`` with a ``DataframeAsset``.

    **Why not** ``PandasFilesystemDatasource``?
    GX 0.18.x calls ``asset.test_connection()`` immediately when you register a
    filesystem asset, which requires matching ``.parquet`` files to already exist
    on disk.  During bootstrap no Parquet has been generated yet, so the call
    raises ``TestConnectionError``.

    ``PandasDatasource`` (in-memory) skips that check entirely.  The real
    DataFrame is attached to the batch request at validation time inside
    ``validate_data.run_data_quality_checks()``.

    Args:
        context: Active ``FileDataContext``.
    """
    # Guard: previous failed runs may have persisted a PandasFilesystemDatasource
    # under the same name in great_expectations.yml.  GX refuses to update a
    # datasource to a different type, so we must delete it first.
    try:
        existing = context.get_datasource(DATASOURCE_NAME)
        if type(existing).__name__ != "PandasDatasource":
            logger.warning(
                "Removing stale '%s' datasource (was %s — must be PandasDatasource).",
                DATASOURCE_NAME,
                type(existing).__name__,
            )
            context.delete_datasource(DATASOURCE_NAME)
    except Exception:
        pass  # Datasource absent — nothing to clean up

    logger.info("Registering in-memory PandasDatasource '%s'…", DATASOURCE_NAME)
    datasource = context.sources.add_or_update_pandas(name=DATASOURCE_NAME)

    _registered = {a.name for a in datasource.assets}
    if ASSET_NAME not in _registered:
        datasource.add_dataframe_asset(name=ASSET_NAME)

    logger.info("DataframeAsset '%s' registered (DataFrame injected at runtime).", ASSET_NAME)


# ─── Step 3: Expectation Suite ───────────────────────────────────────────────

def _build_expectations() -> list[ExpectationConfiguration]:
    """Returns the ordered list of expectation configurations for the suite.

    Organised into four business-rule categories so that Data Docs renders
    a structured summary:

    * **integrity**  — join-key null checks (a null here breaks the star schema)
    * **domain**     — categorical value-set constraints
    * **measure**    — numeric range constraints on fact-table measures
    * **volume**     — row-count bounds to catch broken JOINs early

    Returns:
        List of ``ExpectationConfiguration`` objects ready to be added to the
        suite via ``suite.add_expectation()``.
    """
    return [
        # ── Integrity: join keys must never be null ───────────────────────────
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": COL_YEAR},
            meta={
                "category": "integrity",
                "rule": "vigencia_join_key",
                "impact": "CRITICAL — null vigencia breaks dim_period FK resolution",
            },
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": COL_DEPT_CODE},
            meta={
                "category": "integrity",
                "rule": "codigo_departamento_join_key",
                "impact": "CRITICAL — null dept code breaks dim_geography SK lookup",
            },
        ),

        # ── Domain: categorical columns must stay within approved value sets ──
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": COL_SECTOR,
                "value_set": SECTOR_IES_ALLOWED,
                "mostly": 1.0,   # zero tolerance — any rogue value contaminates analysis
            },
            meta={
                "category": "domain",
                "rule": "sector_ies_allowed_values",
                "approved_values": SECTOR_IES_ALLOWED,
            },
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": COL_ESTRATO,
                "value_set": ESTRATO_ALLOWED,
                "mostly": 0.95,  # 5 % tolerance for strata 4–6 in boundary years
            },
            meta={
                "category": "domain",
                "rule": "estrato_target_population",
                "note": "Analysis targets strata 1–3; strata 4–6 are valid but out-of-scope",
            },
        ),

        # ── Measure: fact-table measures must be non-negative ─────────────────
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": COL_BENEFICIARIES,
                "min_value": 0,
                "max_value": None,   # no hard upper ceiling
                "mostly": 1.0,
            },
            meta={
                "category": "measure",
                "rule": "non_negative_beneficiaries",
                "impact": "A negative value indicates a data-entry error in the source",
            },
        ),

        # ── Volume: row count bounds for the merged flat table ────────────────
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={
                "min_value": ROW_COUNT_MIN,
                "max_value": ROW_COUNT_MAX,
            },
            meta={
                "category": "volume",
                "rule": "row_count_sanity_check",
                "note": (
                    f"< {ROW_COUNT_MIN:,} rows → broken JOIN or missing source file. "
                    f"> {ROW_COUNT_MAX:,} rows → unintended cross-join."
                ),
            },
        ),
    ]


def build_expectation_suite(context: gx.DataContext) -> None:
    """Creates (or fully overwrites) the ``icetex_macro_suite`` and saves it to disk.

    The suite file is written to::

        gx/expectations/icetex_macro_suite.json

    Calling this function a second time replaces all expectations atomically —
    there are no stale rules left over from a previous version of the suite.

    Args:
        context: Active ``FileDataContext``.

    Raises:
        OSError: If the expectations directory is not writable.
    """
    logger.info("Building ExpectationSuite: '%s'", SUITE_NAME)

    suite = context.add_or_update_expectation_suite(
        expectation_suite_name=SUITE_NAME
    )

    # Wipe any pre-existing expectations before rebuilding — guarantees
    # that a re-run never leaves orphaned rules from deleted requirements.
    suite.expectations = []

    for expectation in _build_expectations():
        suite.add_expectation(expectation)

    context.save_expectation_suite(suite)

    suite_path = GX_ROOT / "expectations" / f"{SUITE_NAME}.json"
    if suite_path.exists():
        logger.info("Suite persisted (%d expectations) → %s", len(suite.expectations), suite_path)
    else:
        logger.warning(
            "Suite save returned no error, but file not found at expected path: %s",
            suite_path,
        )


# ─── Step 4: Checkpoint ──────────────────────────────────────────────────────

def configure_checkpoint(context: gx.DataContext) -> None:
    """Registers the ``icetex_macro_checkpoint`` with its action list.

    The checkpoint defines *what happens after validation* (store result,
    update Data Docs) but does **not** hard-code a batch request.  The batch
    is injected at runtime by ``validate_data.run_data_quality_checks()``,
    keeping the checkpoint reusable across different file paths.

    Actions registered:

    1. ``StoreValidationResultAction`` — writes the JSON result to
       ``gx/uncommitted/validations/``.
    2. ``UpdateDataDocsAction`` — regenerates the HTML report in
       ``gx/uncommitted/data_docs/local_site/``.

    Args:
        context: Active ``FileDataContext``.
    """
    logger.info("Configuring checkpoint: '%s'", CHECKPOINT_NAME)

    import pandas as pd  # local import — only needed here for the placeholder

    # GX requires at least one serialisable validation to store the checkpoint.
    # We supply an empty-DataFrame placeholder; the real DataFrame is injected
    # at runtime in validate_data.py via run_checkpoint(validations=[...]).
    datasource = context.get_datasource(DATASOURCE_NAME)
    asset = datasource.get_asset(ASSET_NAME)
    default_batch_request = asset.build_batch_request(dataframe=pd.DataFrame())

    context.add_or_update_checkpoint(
        name=CHECKPOINT_NAME,
        validations=[
            {
                "batch_request": default_batch_request,
                "expectation_suite_name": SUITE_NAME,
            }
        ],
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                    "site_names": ["local_site"],
                },
            },
        ],
        run_name_template="%Y%m%d_%H%M%S-icetex-validation",
    )
    logger.info("Checkpoint '%s' saved to gx/checkpoints/.", CHECKPOINT_NAME)


# ─── Orchestrator ─────────────────────────────────────────────────────────────

def setup_all() -> None:
    """Runs the full bootstrap sequence: context → datasource → suite → checkpoint.

    All four steps are idempotent.  Safe to re-run after editing suite rules.
    """
    logger.info("=" * 68)
    logger.info("Great Expectations Bootstrap — ICETEX Macro Pipeline")
    logger.info("GX version : %s", gx.__version__)
    logger.info("GX root    : %s", GX_ROOT)
    logger.info("=" * 68)

    context = initialise_context()
    configure_datasource(context)
    build_expectation_suite(context)
    configure_checkpoint(context)

    data_docs_path = GX_ROOT / "uncommitted" / "data_docs" / "local_site" / "index.html"
    logger.info("=" * 68)
    logger.info("Bootstrap complete.")
    logger.info("Next steps:")
    logger.info("  1. git add gx/ && git commit -m 'feat: add GX expectations suite'")
    logger.info("  2. python src/validate_data.py  (local smoke-test)")
    logger.info("  3. Data Docs (after first validation): %s", data_docs_path)
    logger.info("=" * 68)


if __name__ == "__main__":
    try:
        setup_all()
    except Exception as exc:
        logger.error("Bootstrap failed: %s", exc, exc_info=True)
        sys.exit(1)
