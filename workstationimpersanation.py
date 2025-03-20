from google.cloud import bigquery
from google.auth import default, impersonated_credentials
import requests
import os

def get_workstation_service_account():
    """Gets the Workstation's service account email from the metadata server."""
    try:
        metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email"
        headers = {"Metadata-Flavor": "Google"}
        response = requests.get(metadata_url, headers=headers, timeout=5)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error getting service account from metadata server: {e}")
        return None

def get_active_credentials_info():
    """Gets information about the active credentials being used."""
    try:
        credentials, project_id = default()
        if hasattr(credentials, 'service_account_email'):
            print(f"Using service account: {credentials.service_account_email}")
            return credentials.service_account_email, "Service Account"
        elif hasattr(credentials, 'client_id'):
            print("Using user credentials.")
            return "User Credentials", "User Account"
        else:
            print("Unknown credential type.")
            return "Unknown", "Unknown"

    except Exception as e:
        print(f"Error getting credentials info: {e}")
        return "Error", "Error"

def run_query_with_impersonation(target_service_account, query_string, project_id):
    """Runs a BigQuery query using impersonated credentials."""
    try:
        creds, _ = default()
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=creds,
            target_principal=target_service_account,
            target_scopes=['https://www.googleapis.com/auth/bigquery'],  # More specific scope
            lifetime=3600
        )
        bq_client = bigquery.Client(credentials=impersonated_creds, project=project_id)
        query_job = bq_client.query(query_string)
        results = query_job.result()
        return results

    except Exception as e:
        print(f"Error running query with impersonation: {e}")
        return None

# --- Main Execution ---

# 1. Get the Workstation's service account email (using metadata server)
workstation_sa_email = get_workstation_service_account()

if workstation_sa_email is None:
    print("Error: Could not determine Workstation service account.")
    exit(1)

print(f"Workstation Service Account: {workstation_sa_email}")

# 2. (Optional) Check the initially active credentials.
print("Checking default credentials:")
get_active_credentials_info()

# 3. Example query (replace with your actual DML operation)
project_id = "your-project-id"  # Replace with your project ID
query = """
    DELETE FROM `your-project-id.your_dataset.your_table`
    WHERE data_url = 'some_url' AND timestamp = '2024-01-01T00:00:00'
"""  # Example DELETE query

# 4. Run the query with impersonation
results = run_query_with_impersonation(workstation_sa_email, query, project_id)

if results:
    print("Query executed successfully (using impersonation).") # No rows will be printed for DML
    # Note: DML statements (DELETE, UPDATE, INSERT) don't return data rows
    # like SELECT queries do.  The 'results' object will still be valid
    # if the query executed successfully, but you won't iterate through rows.
    # To see the number of rows affected, you can inspect the job stats:
    # print(f"Rows affected: {results.num_dml_affected_rows}") # AttributeError: 'object' has no attribute 'num_dml_affected_rows'
    # results contains below attribute, we can get the valye by calling the result() method
    # print(dir(results))

# --- Example of a SELECT query (for comparison) ---
select_query = "SELECT * FROM `your-project-id.your_dataset.your_table` LIMIT 5" #select query
select_results = run_query_with_impersonation(workstation_sa_email, select_query, project_id)
if select_results:
    print("SELECT query results (using impersonation):")
    for row in select_results:
        print(row)
