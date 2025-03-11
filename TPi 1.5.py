import json
import os
import zipfile
import requests
from datetime import datetime, timedelta
from io import BytesIO
from google.cloud import storage, bigquery, secretmanager


def get_credentials(environment, config):
    """Retrieves credentials."""
    if environment == 'production':
        try:
            client = secretmanager.SecretManagerServiceClient()
            project_id = config['api_credentials']['project_id']
            client_id_path = f"projects/{project_id}/secrets/{config['api_credentials']['secret_manager_client_id']}/versions/latest"
            client_secret_path = f"projects/{project_id}/secrets/{config['api_credentials']['secret_manager_client_secret']}/versions/latest"
            client_id = client.access_secret_version(request={"name": client_id_path}).payload.data.decode("UTF-8")
            client_secret = client.access_secret_version(request={"name": client_secret_path}).payload.data.decode("UTF-8")
            return (client_id, client_secret), project_id
        except Exception as e:
            raise Exception(f"Error accessing Secret Manager: {e}")

    elif environment in ('development', 'staging'):
        return (config['api_credentials']['client_id'],
                config['api_credentials']['client_secret']), config['api_credentials']['project_id']
    else:
        raise ValueError("Invalid environment specified.")

def get_token(credentials, auth_url):
    """Fetches an access token."""
    try:
        if isinstance(credentials, tuple):
            client_id, client_secret = credentials
            response = requests.post(
                auth_url,
                data={"grant_type": "client_credentials"},
                auth=(client_id, client_secret),
                timeout=30
            )
            response.raise_for_status()
            return response.json()["access_token"]
        else:
            raise TypeError("Invalid credentials type.")

    except requests.exceptions.RequestException as e:
        raise Exception(f"Error getting token: {e}")
    except (KeyError, ValueError) as e:
      raise Exception(f"Error getting token: {e}")

def get_scope_id(token, scope_url):
    """Fetches the scope ID."""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(scope_url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()["scope_id"]
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error getting scope ID: {e}")
    except (KeyError, ValueError) as e:
      raise Exception(f"Error getting scope ID: {e}")

def download_and_upload_zip(credentials, project_id, scope_id, data_url, timestamp, bucket_name, destination_blob_prefix, bq_table, auth_url):
    """Downloads, uploads, and manages records."""
    formatted_timestamp = timestamp.strftime("%Y-%m-%d-%H-%M-%S")
    full_data_url = f"{data_url}?timestamp={formatted_timestamp}"
    destination_blob_name = f"{destination_blob_prefix}/{formatted_timestamp}.zip"

    token = get_token(credentials, auth_url)
    headers = {"Authorization": f"Bearer {token}", "X-Scope-Id": str(scope_id)}

    try:
        response = requests.get(full_data_url, headers=headers, stream=True, timeout=60)
        response.raise_for_status()

        zip_data = BytesIO(response.content)
        if not zipfile.is_zipfile(zip_data):
            raise Exception("Response is not a valid zip file.")
        zip_data.seek(0)

        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        try:
            blob.upload_from_file(zip_data, content_type='application/zip')
            print(f"File uploaded to gs://{bucket_name}/{destination_blob_name}")
            # Record successful upload and ensure only one success record
            update_or_insert_success_record(bq_table, data_url, timestamp, credentials, project_id)
            delete_record_if_exists(bq_table, data_url, timestamp,credentials, project_id)


        except Exception as upload_error:
            raise Exception(f"Error uploading to GCS: {upload_error}")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code in (404, 500):
            print(f"Ignoring HTTP error {e.response.status_code} for {full_data_url}")
            # Record ignored attempts
            insert_record(bq_table, data_url, timestamp, "IGNORED", str(e), credentials, project_id)
        else:
            error_message = f"HTTP Error: {e.response.status_code} - {e.response.text}"
            print(f"Error processing {full_data_url}: {error_message}")
            insert_record(bq_table, data_url, timestamp, "FAILURE", error_message, credentials, project_id)
            raise

    except requests.exceptions.RequestException as e:
        error_message = f"Request Error: {e}"
        print(f"Error processing {full_data_url}: {error_message}")
        insert_record(bq_table, data_url, timestamp, "FAILURE", error_message, credentials, project_id)
        raise
    except Exception as e:
        error_message = str(e)
        print(f"Error processing {full_data_url}: {error_message}")
        insert_record(bq_table, data_url, timestamp, "FAILURE", error_message, credentials, project_id)
        raise


def insert_record(bq_table, data_url, timestamp, status, error_message, credentials, project_id):
    """Inserts a failure/ignored record into BigQuery."""
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.get_table(bq_table)

    rows_to_insert = [
        {
            "data_url": data_url,
            "timestamp": timestamp.isoformat(),
            "status": status,
            "error_message": error_message,
            "load_ts": datetime.utcnow().isoformat(),
        }
    ]
    errors = bq_client.insert_rows(table_ref, rows_to_insert)
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")

def delete_record_if_exists(bq_table, data_url, timestamp, credentials, project_id):
    """Deletes a failed record from BigQuery if it exists."""
    bq_client = bigquery.Client(project=project_id)
    delete_query = f"""
        DELETE FROM `{bq_table}`
        WHERE data_url = @data_url
          AND timestamp = @timestamp
          AND status = 'FAILURE'
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data_url", "STRING", data_url),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", timestamp),
        ]
    )
    query_job = bq_client.query(delete_query, job_config=job_config)
    try:
        query_job.result()
        print(f"Deleted failed record for {data_url} at {timestamp}")
    except Exception as e:
        print(f"Error deleting record: {e}")


def update_or_insert_success_record(bq_table, data_url, timestamp, credentials, project_id):
    """Updates or inserts the success record, ensuring only one exists."""
    bq_client = bigquery.Client(project=project_id)

    # First, try to update.  If no rows are updated, then insert.
    query = f"""
        UPDATE `{bq_table}`
        SET timestamp = @timestamp, status = 'SUCCESS', error_message = NULL, load_ts = CURRENT_TIMESTAMP()
        WHERE data_url = @data_url AND status = 'SUCCESS';

        IF @@row_count = 0 THEN
            INSERT INTO `{bq_table}` (data_url, timestamp, status, error_message, load_ts)
            VALUES (@data_url, @timestamp, 'SUCCESS', NULL, CURRENT_TIMESTAMP());
        END IF;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data_url", "STRING", data_url),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", timestamp),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    try:
        query_job.result()
        print(f"Updated/Inserted success record for {data_url} at {timestamp}")
    except Exception as e:
        print(f"Error updating/inserting success record: {e}")



def get_timestamps_by_status(bq_table, data_url, start_time, end_time, status, credentials, project_id):
    """Retrieves a list of timestamps with a specific status."""
    bq_client = bigquery.Client(project=project_id)
    query = f"""
        SELECT timestamp
        FROM `{bq_table}`
        WHERE data_url = @data_url
          AND timestamp >= @start_time
          AND timestamp <= @end_time
          AND status = @status
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data_url", "STRING", data_url),
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
            bigquery.ScalarQueryParameter("status", "STRING", status),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    results = query_job.result()
    return [row.timestamp for row in results]


def get_last_successful_timestamp(bq_table, data_url, credentials, project_id):
    """Retrieves the last successful timestamp, handling multiple success records."""
    bq_client = bigquery.Client(project=project_id)
    query = f"""
        SELECT timestamp
        FROM `{bq_table}`
        WHERE data_url = @data_url
          AND status = 'SUCCESS'
        ORDER BY timestamp DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data_url", "STRING", data_url),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    try:
        results = query_job.result()
        rows = list(results)
        if rows:
            return rows[0].timestamp
        else:
            return None
    except Exception as e:
        print(f"Error getting last successful timestamp: {e}")
        return None


def process_data_url(config, data_url_config, current_time):
    """Processes a single data URL."""
    credentials, project_id = get_credentials(config['environment'], config)
    scope_id = get_scope_id(get_token(credentials, config['api_endpoints']['auth_url']), config['api_endpoints']['scope_url'])
    data_url = data_url_config['url']
    bucket_name = config['gcs_bucket']
    destination_blob_prefix = data_url_config.get('destination_blob_prefix', config.get('destination_blob_name', ''))
    bq_table = config['bigquery_table']
    auth_url = config["api_endpoints"]["auth_url"]

    last_successful_time = get_last_successful_timestamp(bq_table, data_url, credentials, project_id)
    if last_successful_time is None:
        last_successful_time = current_time - timedelta(hours=2)
    else:
        print(f"Last successful time for {data_url}: {last_successful_time}")

    failed_timestamps = get_timestamps_by_status(
        bq_table, data_url, last_successful_time, current_time, "FAILURE", credentials, project_id
    )

    current_timestamp = last_successful_time + timedelta(minutes=5)
    while current_timestamp <= current_time:
        if current_timestamp not in failed_timestamps:
            failed_timestamps.append(current_timestamp)
        current_timestamp += timedelta(minutes=5)

    for timestamp in failed_timestamps:
        download_and_upload_zip(
            credentials, project_id, scope_id, data_url, timestamp, bucket_name,
            destination_blob_prefix, bq_table, auth_url
        )


def main():
    """Main function."""
    config_file = "config.json"
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading config file: {e}")
        return
    current_time = datetime.utcnow()

    for data_url_config in config['data_urls']:
        try:
            process_data_url(config, data_url_config, current_time)
        except Exception as e:
            print(f"Error processing data URL {data_url_config.get('name', data_url_config['url'])}: {e}")

if __name__ == "__main__":
    main()
