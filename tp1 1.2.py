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
            # Using Secret Manager (Best Practice for Production)
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
        # Using credentials directly from config (OK for dev/staging)
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
    """Downloads, uploads, and manages failed records."""
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
        blob.upload_from_file(zip_data, content_type='application/zip')

        print(f"File uploaded to gs://{bucket_name}/{destination_blob_name}")

        # Delete successful attempts from BigQuery (if they were previously failures)
        delete_failed_record(bq_table, data_url, timestamp, credentials, project_id)

    except Exception as e:
        error_message = str(e)
        print(f"Error processing {full_data_url}: {error_message}")
        # Log the failure
        insert_failed_record(bq_table, data_url, timestamp, error_message, credentials, project_id)
        raise  # Re-raise to stop further processing for this timestamp


def insert_failed_record(bq_table, data_url, timestamp, error_message, credentials, project_id):
    """Inserts a failure record into the BigQuery table."""

    bq_client = bigquery.Client(project=project_id)
    table = bq_client.get_table(bq_table)  # Get the table object

    rows_to_insert = [
        {
            "data_url": data_url,
            "timestamp": timestamp.isoformat(),
            "error_message": error_message,
            "load_ts": datetime.utcnow().isoformat(),
        }
    ]
    errors = bq_client.insert_rows(table, rows_to_insert)  # Pass table object
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")

def delete_failed_record(bq_table, data_url, timestamp, credentials, project_id):
    """Deletes a failed record from BigQuery upon successful retry."""
    from google.cloud import bigquery

    if isinstance(credentials, tuple):
      client = bigquery.Client(project=project_id)
    else:
      client = bigquery.Client(credentials=credentials, project=project_id)

    delete_query = f"""
        DELETE FROM `{bq_table}`
        WHERE data_url = @data_url
        AND timestamp = @timestamp
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data_url", "STRING", data_url),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", timestamp),
        ]
    )
    try:
      query_job = client.query(delete_query, job_config=job_config)
      query_job.result()
      print(f"Deleted failed record for {data_url} at {timestamp}")
    except Exception as e:
      print(f"Failed to delete the record from bigquery table:{e}")



def get_failed_timestamps(bq_table, data_url, start_time, end_time, credentials, project_id):
    """Retrieves a list of failed timestamps from BigQuery."""

    from google.cloud import bigquery

    if isinstance(credentials, tuple):
      client = bigquery.Client(project=project_id)
    else:
      client = bigquery.Client(credentials=credentials, project=project_id)

    query = f"""
        SELECT timestamp
        FROM `{bq_table}`
        WHERE data_url = @data_url
        AND timestamp >= @start_time
        AND timestamp <= @end_time
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data_url", "STRING", data_url),
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    return [row.timestamp for row in results]




def process_data_url(config, data_url_config, last_successful_time, current_time):
    """Processes a single data URL, handling retries."""

    credentials, project_id = get_credentials(config['environment'], config)
    scope_id = get_scope_id(get_token(credentials, config['api_endpoints']['auth_url']), config['api_endpoints']['scope_url'])
    data_url = data_url_config['url']
    bucket_name = config['gcs_bucket']
    destination_blob_prefix = data_url_config.get('destination_blob_prefix', config.get('destination_blob_name', ''))  # Default handling
    bq_table = config['bigquery_table']
    auth_url = config["api_endpoints"]["auth_url"]

    # Get failed timestamps
    failed_timestamps = get_failed_timestamps(
        bq_table, data_url, last_successful_time, current_time, credentials, project_id
    )

    # Add new timestamps
    current_timestamp = last_successful_time + timedelta(minutes=5)
    while current_timestamp <= current_time:
        if current_timestamp not in failed_timestamps: #check for existing timestamps
          failed_timestamps.append(current_timestamp)
        current_timestamp += timedelta(minutes=5)

    # Process all timestamps (new and retries)
    for timestamp in failed_timestamps:
        download_and_upload_zip(
            credentials, project_id, scope_id, data_url, timestamp, bucket_name,
            destination_blob_prefix, bq_table, auth_url
        )


def main():
    """Main function to load config and process data URLs."""
    config_file = "config.json"  # Or get from command line argument
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading config file: {e}")
        return

    current_time = datetime.utcnow()
    # For simplicity, let's assume a 2-hour lookback for the first run.
    # In a real application, you'd store and retrieve the last successful
    # run time.
    last_successful_time = current_time - timedelta(hours=2)

    for data_url_config in config['data_urls']:
        try:
            process_data_url(config, data_url_config, last_successful_time, current_time)
        except Exception as e:
            print(f"Error processing data URL {data_url_config.get('name', data_url_config['url'])}: {e}")
            # Consider adding more sophisticated error handling/reporting here.
            #  For example, sending an email, logging to a central system, etc.

if __name__ == "__main__":
    main()