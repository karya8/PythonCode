from __future__ import annotations

import datetime
import logging
import os
from typing import Any

import pendulum
import yaml
import json
import shutil

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

BQ_TABLE = "{{ dag_run.conf.bigquery_table }}"
BQ_DATASET = "{{ dag_run.conf.bigquery_dataset }}"
BQ_PROJECT = "{{ dag_run.conf.bigquery_project }}"


def _round_down_to_nearest_five(minute: int) -> int:
    """Rounds a minute value down to the nearest multiple of 5."""
    return minute - (minute % 5)


@task
def read_main_config(main_config_file: str) -> dict[str, Any]:
    """Reads the main configuration from a JSON file."""
    config_file_path = os.path.join(os.path.dirname(__file__), main_config_file)
    with open(config_file_path, "r") as f:
        config_data = json.load(f)
    return config_data


@task
def read_load_requests_file(config_base_path: str, load_requests_file: str) -> list[dict[str, Any]]:
    """Reads the YAML file containing a list of load requests."""
    config_file_path = os.path.join(os.path.dirname(__file__), config_base_path, load_requests_file)
    with open(config_file_path, "r") as f:
        load_requests_data = yaml.safe_load(f)
    return load_requests_data.get("load_requests", [])


@task
def generate_timestamps(load_request: dict[str, Any]) -> list[str]:
    """Generates 5-minute timestamp intervals."""
    start_ts_str = load_request["start_timestamp"]
    end_ts_str = load_request["end_timestamp"]

    start_ts = pendulum.parse(start_ts_str)
    end_ts = pendulum.parse(end_ts_str)

    timestamps = []
    current_ts = start_ts.replace(minute=_round_down_to_nearest_five(start_ts.minute), second=0, microsecond=0)

    while current_ts <= end_ts:
        timestamps.append(current_ts.to_iso8601_string())
        current_ts = current_ts.add(minutes=5)

    return timestamps


@task
def fetch_existing_timestamps_bq(load_requests: list[dict[str, Any]], bq_project: str, bq_dataset: str, bq_table: str) -> dict[str, set[str]]:
    """Fetches existing timestamps within the load request ranges from BigQuery."""
    from google.cloud import bigquery

    client = bigquery.Client(project=bq_project)
    table_id = f"{bq_project}.{bq_dataset}.{bq_table}"
    existing_timestamps = {}

    for req in load_requests:
        schema_name = req["schema_name"]
        start_ts_str = req["start_timestamp"]
        end_ts_str = req["end_timestamp"]

        query = f"""
            SELECT timestamp
            FROM `{table_id}`
            WHERE schema_name = '{schema_name}'
              AND timestamp >= TIMESTAMP('{start_ts_str}')
              AND timestamp <= TIMESTAMP('{end_ts_str}')
        """
        query_job = client.query(query)
        results = query_job.result()

        if schema_name not in existing_timestamps:
            existing_timestamps[schema_name] = set()
        for row in results:
            existing_timestamps[schema_name].add(row["timestamp"].isoformat())

    return existing_timestamps


@task
def process_and_stage_timestamps(load_requests: list[dict[str, Any]], all_timestamps: dict[str, list[str]], existing_timestamps: dict[str, set[str]]) -> list[dict[str, Any]]:
    """Processes generated timestamps and stages data for insertion."""
    insert_data = []
    for req in load_requests:
        schema_name = req["schema_name"]
        load_strategy = req["load_strategy"]
        generated_ts = all_timestamps.get(schema_name, [])

        for ts in generated_ts:
            if schema_name not in existing_timestamps or ts not in existing_timestamps[schema_name]:
                insert_data.append({
                    "schema_name": schema_name,
                    "timestamp": ts,
                    "status": "load",
                    "retry": 0
                })
            elif load_strategy.lower() == "overwrite":
                insert_data.append({
                    "schema_name": schema_name,
                    "timestamp": ts,
                    "status": "load",
                    "retry": 1  # Assuming overwrite implies a form of retry/update
                })
            else:
                logging.info(f"Skipping timestamp: {ts} for schema: {schema_name} as it exists and load strategy is not overwrite.")
    return insert_data


@task
def insert_timestamps_bq(insert_data: list[dict[str, Any]], bq_project: str, bq_dataset: str, bq_table: str):
    """Inserts all staged timestamp data into BigQuery in one go."""
    from google.cloud import bigquery

    if not insert_data:
        logging.info("No new timestamps to insert.")
        return

    client = bigquery.Client(project=bq_project)
    table_id = f"{bq_project}.{bq_dataset}.{bq_table}"
    table = client.get_table(table_id)  # Get the table schema

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND  # Assuming you always want to append

    json_data = [json.dumps(row) for row in insert_data]
    job = client.load_from_json(json_data, table_id, job_config=job_config)
    job.result()  # Wait for the load job to complete

    logging.info(f"Inserted {len(insert_data)} new timestamps into {table_id}")


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def archive_load_requests_file(config_base_path: str, load_requests_file: str, archive_base_path: str):
    """Moves the processed load requests file to the specified archive folder."""
    source_path = os.path.join(os.path.dirname(__file__), config_base_path, load_requests_file)
    archive_folder = os.path.join(os.path.dirname(__file__), archive_base_path)
    destination_path = os.path.join(archive_folder, load_requests_file)
    os.makedirs(archive_folder, exist_ok=True)
    shutil.move(source_path, destination_path)
    logging.info(f"Moved load requests file '{load_requests_file}' to '{archive_folder}'")


@dag(
    dag_id="{{ dag_run.conf.dag_name }}",
    schedule="30 16 * * *",  # Runs daily at 12:30 PM EDT (16:30 UTC)
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["timestamp_processing"],
    default_args={
        "retries": None,  # Will be read from config
        "retry_delay": None,  # Will be read from config
    },
)
def timestamp_processing_dag():
    main_config_task = read_main_config(main_config_file="config/main_config.json")
    load_requests = read_load_requests_file(
        config_base_path=main_config_task["config_folder"],
        load_requests_file=main_config_task["load_requests_file"],
    )

    # Set default_args here, after reading the config
    dag_kwargs = {
        "default_args": {
            "retries": main_config_task.get("retries"),
            "retry_delay": datetime.timedelta(minutes=main_config_task.get("retry_delay_minutes")) if main_config_task.get("
