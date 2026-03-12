"""
run_pipeline.py — SkillPulse India M7 Pipeline Entrypoint
==========================================================
Replaces the Airflow DAG. Runs the full medallion pipeline in sequence:
  1. Bronze ingestion  (JSearch API → GCS)
  2. Silver enrichment (Gemini AI → BigQuery)
  3. BQ load           (Silver JSON → BigQuery stg_job_postings)
  4. dbt build         (Gold marts)

Designed to run as a Cloud Run Job. All config is injected via env vars.
DRY_RUN=true skips live API calls (already guarded in each step).
"""

import os
import sys
import uuid
import logging
import subprocess
from datetime import datetime, timezone

from dotenv import load_dotenv  # no-op in Cloud Run (env vars already set)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("run_pipeline")

# ── Ensure project root is on PYTHONPATH ────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ── Pipeline Run Tracking ───────────────────────────────────────────────────
def _bq_client():
    """Lazy BigQuery client — avoids import errors on dry-run local tests."""
    from google.cloud import bigquery  # noqa: PLC0415

    return bigquery.Client(project=os.getenv("BQ_PROJECT_ID"))


def write_pipeline_run(run_id: str, status: str, extra: dict | None = None) -> None:
    """
    Upserts a row to skillpulse_gold.pipeline_runs.
    Called at START (status=RUNNING) and END (status=SUCCESS/FAILED).
    """
    project_id = os.getenv("BQ_PROJECT_ID")
    gold_dataset = os.getenv("BQ_DATASET_GOLD", "skillpulse_gold")
    table_id = f"{project_id}.{gold_dataset}.pipeline_runs"

    row = {
        "run_id": run_id,
        "run_date": os.getenv(
            "PIPELINE_RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ),
        "dag_trigger_time": datetime.now(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        ),
        "dry_run_mode": os.getenv("DRY_RUN", "false").lower() == "true",
        "status": status,
    }
    if extra:
        row.update(extra)

    try:
        client = _bq_client()
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            logger.warning(f"pipeline_runs insert warnings: {errors}")
        else:
            logger.info(f"pipeline_runs ← status={status}")
    except Exception as exc:
        # Never crash the pipeline just because observability failed
        logger.error(f"Failed to write pipeline_runs ({status}): {exc}")


# ── Step Runners ─────────────────────────────────────────────────────────────
def step_ingest_bronze() -> None:
    logger.info("=" * 60)
    logger.info("STEP 1/4 — Bronze Ingestion (JSearch → GCS)")
    logger.info("=" * 60)
    from src.ingestion.fetch_jobs_bronze import main as ingest_main  # noqa: PLC0415

    ingest_main()
    logger.info("✅ Bronze ingestion complete.")


def step_enrich_silver() -> None:
    logger.info("=" * 60)
    logger.info("STEP 2/4 — Silver Enrichment (Gemini AI)")
    logger.info("=" * 60)
    from src.enrichment.process_silver import process_bronze_to_silver  # noqa: PLC0415

    process_bronze_to_silver()
    logger.info("✅ Silver enrichment complete.")


def step_load_bq() -> None:
    logger.info("=" * 60)
    logger.info("STEP 3/4 — BigQuery Load (Silver → stg_job_postings)")
    logger.info("=" * 60)
    from src.warehouse.load_silver_to_bq import load_silver_to_bq  # noqa: PLC0415

    load_silver_to_bq()
    logger.info("✅ BigQuery load complete.")


def step_dbt_build() -> None:
    """
    Runs: dbt clean && dbt deps && dbt build --target prod
    Working directory: dbt_project/
    """
    logger.info("=" * 60)
    logger.info("STEP 4/4 — dbt Build (Gold marts, target=prod)")
    logger.info("=" * 60)

    dbt_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dbt_project")

    for cmd in [
        ["dbt", "clean"],
        ["dbt", "deps"],
        ["dbt", "build", "--target", "prod"],
    ]:
        logger.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            cwd=dbt_dir,
            capture_output=False,  # stream directly to Cloud Logging
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"dbt command failed (exit {result.returncode}): {' '.join(cmd)}"
            )

    logger.info("✅ dbt build complete.")


# ── Main Orchestrator ────────────────────────────────────────────────────────
def main() -> None:
    load_dotenv()  # no-op in Cloud Run, loads local .env for development

    run_id = str(uuid.uuid4())
    is_dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
    start_time = datetime.now(timezone.utc)

    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║        SkillPulse India — Daily Pipeline (M7)           ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Run ID   : {run_id}")
    logger.info(f"DRY_RUN  : {is_dry_run}")
    logger.info(f"Start UTC: {start_time.isoformat()}")

    # START marker in pipeline_runs
    write_pipeline_run(run_id, "RUNNING")

    steps = [
        ("ingest_bronze", step_ingest_bronze),
        ("enrich_silver", step_enrich_silver),
        ("load_bq", step_load_bq),
        ("dbt_build", step_dbt_build),
    ]

    failed_step: str | None = None
    for step_name, step_fn in steps:
        try:
            step_fn()
        except Exception as exc:
            logger.error(f"❌ Step '{step_name}' FAILED: {exc}", exc_info=True)
            failed_step = step_name
            break

    end_time = datetime.now(timezone.utc)
    duration_seconds = (end_time - start_time).total_seconds()
    final_status = "FAILED" if failed_step else "SUCCESS"

    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info(f"║  Pipeline {final_status:<10} | {duration_seconds:.1f}s elapsed" + " " * (24 - len(f"{duration_seconds:.1f}")) + "║")
    logger.info("╚══════════════════════════════════════════════════════════╝")

    # END marker in pipeline_runs
    write_pipeline_run(
        run_id,
        final_status,
        extra={
            "run_duration_seconds": duration_seconds,
            "failed_step": failed_step,
        },
    )

    if failed_step:
        sys.exit(1)  # Non-zero exit → Cloud Run marks execution as FAILED


if __name__ == "__main__":
    main()
