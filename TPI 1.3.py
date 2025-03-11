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
            response.raise_for_status()  # This will raise an exception for 4xx or 5xx errors
            return response.json()["access_token"]
        else:
            raise TypeError("Invalid credentials type.")

    except requests.exceptions.RequestException as e:
        raise Exception(f"Error getting token: {e}")  # Catch specific RequestException
    except (KeyError, ValueError) as e:
      raise Exception(f"Error getting token: {e}")

def get_scope_id(token, scope_url):
    """Fetches the scope ID."""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(scope_url, headers=headers, timeout=30)
        response.raise_for_status()  # This will raise an exception for 4xx or 5xx errors
        return response.json()["scope_id"]
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error getting scope ID: {e}")  # Catch specific RequestException
    except (KeyError, ValueError) as e:
      raise Exception(f"Error getting scope ID: {e}")


def download_and_upload_zip(credentials, project_id, scope_id, data_url, timestamp, bucket_name, destination_blob_prefix, bq_table, auth_url):
    """Downloads, uploads, and manages failed records, handling HTTP errors."""
    formatted_timestamp = timestamp.strftime("%Y-%m-%d-%H-%M-%S")
    full_data_url = f"{data_url}?timestamp={formatted_timestamp}"
    destination_blob_name = f"{destination_blob_prefix}/{formatted_timestamp}.zip"

    token = get_token(credentials, auth_url)
    headers = {"Authorization": f"Bearer {token}", "X-Scope-Id": str(scope_id)}

    try:
        response = requests.get(full_data_url, headers=headers, stream=True, timeout=60)
        response.raise_for_status()  # Raises HTTPError for bad requests

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
            delete_failed_record(bq_table, data_url, timestamp, credentials, project_id)
        except Exception as upload_error:
            raise Exception(f"Error uploading to GCS: {upload_error}")

    except requests.exceptions.HTTPError as e:
        # Ignore 404 and 500 errors, but raise for others
        if e.response.status_code in (404, 500):
            print(f"Ignoring HTTP error {e.response.status_code} for {full_data_url}")
            # We *don't* log these to BigQuery, and we don't re-raise.
        else:
             # Handle other HTTP errors (like 403 Forbidden, etc.)
            error_message = f"HTTP Error: {e.response.status_code} - {e.response.text}"
            print(f"Error processing {full_data_url}: {error_message}")
            insert_failed_record(bq_table, data_url, timestamp, error_message, credentials, project_id)
            raise

    except requests.exceptions.RequestException as e:
        error_message = f"Request Error: {e}"
        print(f"Error processing {full_data_url}: {error_message}")
        insert_failed_record(bq_table, data_url, timestamp, error_message, credentials, project_id)
        raise
    except Exception as e:
        error_message = str(e)
        print(f"Error processing {full_data_url}: {error_message}")
        insert_failed_record(bq_table, data_url, timestamp, error_message, credentials, project_id)
        raise



def insert_failed_record(bq_table, data_url, timestamp, error_message, credentials, project_id):
    """Inserts a failure record into BigQuery."""
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.get_table(bq_table) # Get table ref

    rows_to_insert = [
        {
            "data_url": data_url,
            "timestamp": timestamp.isoformat(),
            "error_message": error_message,
            "load_ts": datetime.utcnow().isoformat(),
        }
    ]
    errors = bq_client.insert_rows(table_ref, rows_to_insert)  # Use table ref
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")


def delete_failed_record(bq_table, data_url, timestamp, credentials, project_id):
    """Deletes a failed record from BigQuery upon successful retry."""
    bq_client = bigquery.Client(project=project_id)

    query = f"""
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
    query_job = bq_client.query(query, job_config=job_config)

    try:
        query_job.result()  # Waits for job to complete
        print(f"Deleted failed record for {data_url} at {timestamp}")
    except Exception as e:
        print(f"Error deleting record: {e}")  # More specific error


def get_failed_timestamps(bq_table, data_url, start_time, end_time, credentials, project_id):
    """Retrieves a list of failed timestamps from BigQuery."""
    bq_client = bigquery.Client(project=project_id)

    query = f"""
        SELECT timestamp
        FROM `{bq_table}`
        WHERE data_url = @data_url
        AND timestamp >= @start_time
        AND timestamp <= @end_time
    """
    # Use QueryJobConfig for parameterized queries
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data_url", "STRING", data_url),
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
        ]
    )

    # Execute the query using the client and job config.  This is safer.
    query_job = bq_client.query(query, job_config=job_config)

    #  .result() is needed to wait the query completition and fetch results.
    results = query_job.result()
    return [row.timestamp for row in results]



def process_data_url(config, data_url_config, last_successful_time, current_time):
    """Processes a single data URL, handling retries."""
    credentials, project_id = get_credentials(config['environment'], config)
    scope_id = get_scope_id(get_token(credentials, config['api_endpoints']['auth_url']), config['api_endpoints']['scope_url'])
    data_url = data_url_config['url']
    bucket_name = config['gcs_bucket']
    destination_blob_prefix = data_url_config.get('destination_blob_prefix', config.get('destination_blob_name', ''))
    bq_table = config['bigquery_table']
    auth_url = config["api_endpoints"]["auth_url"]

    failed_timestamps = get_failed_timestamps(
        bq_table, data_url, last_successful_time, current_time, credentials, project_id
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
    """Main function to load config and process data URLs."""
    config_file = "config.json"  # Or get from command line argument
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading config file: {e}")
        return

    current_time = datetime.utcnow()
    last_successful_time = current_time - timedelta(hours=2)

    for data_url_config in config['data_urls']:
        try:
            process_data_url(config, data_url_config, last_successful_time, current_time)
        except Exception as e:
            print(f"Error processing data URL {data_url_config.get('name', data_url_config['url'])}: {e}")

if __name__ == "__main__":
    main()
