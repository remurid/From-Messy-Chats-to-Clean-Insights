# repo.py
#
# Description: This file defines the Dagster repository, which includes all the
# jobs, schedules, and assets for the Telegram data pipeline. It orchestrates
# the entire workflow from scraping to dbt transformation and enrichment.

import os
from pathlib import Path
from dagster import op, job, schedule, get_dagster_logger, Definitions
from dagster_dbt import DbtCliResource, dbt_assets

# --- Import logic from your existing scripts ---
# To make our scripts usable by Dagster, we should refactor them slightly
# to be importable functions. For this example, we'll simulate the execution.
# In a real project, you would import main() from each script.
import subprocess

# --- Configuration ---

# Define the path to your dbt project
DBT_PROJECT_DIR = Path(__file__).parent.joinpath("my_telegram_analytics")

# Configure the dbt resource
dbt = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

# --- dbt Assets ---
# This is the modern, preferred way to integrate dbt with Dagster.
# It automatically creates an op for each dbt model.
@dbt_assets(manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"))
def my_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# --- Dagster Ops ---
# We define each step of our Python-based pipeline as an "op".

@op
def scrape_telegram_data_op():
    """
    Dagster op to run the Telegram scraping script.
    """
    logger = get_dagster_logger()
    logger.info("Starting Telegram scraping process...")
    try:
        # We use subprocess to run your existing script as is.
        # The 'capture_output=True' and 'text=True' arguments allow us to log the script's output.
        result = subprocess.run(
            ["python", "telegram_scraper.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("Script output:\n" + result.stdout)
        logger.info("Telegram scraping completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Telegram scraping script failed with exit code {e.returncode}.")
        logger.error("Error output:\n" + e.stderr)
        raise e
    return True # Return a value to signal success to downstream ops

@op
def load_raw_to_postgres_op(scrape_success: bool):
    """
    Dagster op to run the script that loads raw JSON data into PostgreSQL.
    This op will only run if scrape_telegram_data_op is successful.
    """
    logger = get_dagster_logger()
    if not scrape_success:
        logger.warning("Skipping load to Postgres because scraping failed.")
        return
    
    logger.info("Starting raw data loading process...")
    try:
        result = subprocess.run(
            ["python", "load_raw_to_postgres.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("Script output:\n" + result.stdout)
        logger.info("Raw data loading completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Raw data loading script failed with exit code {e.returncode}.")
        logger.error("Error output:\n" + e.stderr)
        raise e
    return True

@op
def run_yolo_enrichment_op(dbt_success: bool):
    """
    Dagster op to run the YOLO enrichment script.
    This op will only run after the dbt models have been successfully built.
    """
    logger = get_dagster_logger()
    if not dbt_success:
        logger.warning("Skipping YOLO enrichment because dbt build failed.")
        return

    logger.info("Starting YOLO enrichment process...")
    try:
        result = subprocess.run(
            ["python", "run_yolo_enrichment.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("Script output:\n" + result.stdout)
        logger.info("YOLO enrichment completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"YOLO enrichment script failed with exit code {e.returncode}.")
        logger.error("Error output:\n" + e.stderr)
        raise e
    return True


# --- Dagster Job ---
# A "job" is a graph of ops that defines the execution order and dependencies.

@job
def full_telegram_etl_job():
    """
    This job defines the full end-to-end data pipeline.
    1. Scrape data from Telegram.
    2. Load the raw data into PostgreSQL.
    3. Run dbt to transform the data (this is handled by the asset dependency).
    4. Run YOLO enrichment on the images.
    """
    # Note: The dependency for dbt assets is implicit.
    # The `run_yolo_enrichment_op` will depend on the `my_dbt_assets` group.
    # For simplicity in this example, we are showing a more explicit op-based flow.
    # A more advanced pattern would use asset dependencies directly.

    scrape_result = scrape_telegram_data_op()
    load_result = load_raw_to_postgres_op(scrape_result)
    
    # This is a placeholder to show the flow. In a real Dagster asset-based
    # project, the dependency on dbt would be handled differently.
    # For now, let's assume dbt runs after loading. We'll manually trigger it
    # in the UI or use a more advanced setup.
    # The `run_yolo_enrichment_op` would then depend on the `dbt_assets`.
    
    # To keep this example runnable, we'll simplify the job for now.
    # In a real scenario, you'd connect the dbt asset to the enrichment op.
    run_yolo_enrichment_op(load_result)


# --- Dagster Schedule ---
# A "schedule" tells Dagster to run a job at a specific interval.

@schedule(
    job=full_telegram_etl_job,
    cron_schedule="0 5 * * *",  # Run once every day at 5:00 AM
    execution_timezone="Africa/Addis_Ababa",
)
def daily_telegram_pipeline_schedule(context):
    """
    Defines a daily schedule for the main ETL job.
    """
    return {}


# --- Dagster Definitions ---
# This is the main entry point for Dagster, collecting all our definitions.

defs = Definitions(
    assets=[my_dbt_assets],
    jobs=[full_telegram_etl_job],
    schedules=[daily_telegram_pipeline_schedule],
    resources={
        "dbt": dbt,
    },
)
