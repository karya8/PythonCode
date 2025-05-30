from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import socket
import logging
from urllib.parse import urlparse # Import urlparse for hostname extraction

# Configure logging for better visibility in Airflow logs
log = logging.getLogger(__name__)

# --- Configuration ---
# Replace with your actual API endpoints and credentials
TOKEN_API_URL = "https://example.com/api/token"  # Placeholder for your token API
THIRD_PARTY_API_URL = "https://api.example.com/data"  # Placeholder for your third-party API
API_USERNAME = "your_username"  # Placeholder for your API username
API_PASSWORD = "your_password"  # Placeholder for your API password

def complete_api_workflow_task(**kwargs):
    """
    This single task performs the entire workflow:
    1. Gets an authentication token.
    2. Uses the token to call a third-party API.
    3. Prints the IP addresses of the Airflow container and the destination API.
    """
    # --- 1. Get Token from Token API ---
    auth_token = None
    log.info(f"Attempting to get token from: {TOKEN_API_URL}")
    try:
        response = requests.post(
            TOKEN_API_URL,
            json={"username": API_USERNAME, "password": API_PASSWORD},
            timeout=10
        )
        response.raise_for_status()
        token_data = response.json()
        auth_token = token_data.get("access_token")

        if auth_token:
            log.info("Successfully retrieved authentication token.")
        else:
            raise ValueError("Authentication token not found in response.")

    except requests.exceptions.RequestException as e:
        log.error(f"Error getting token: {e}")
        raise
    except ValueError as e:
        log.error(f"Token retrieval failed: {e}")
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred during token retrieval: {e}")
        raise

    # --- 2. Call Third-Party API using the Token ---
    if not auth_token:
        # This check is technically redundant due to the 'raise' above, but good for clarity
        log.error("Authentication token is missing, cannot proceed with API call.")
        return # Or raise another error if desired

    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    log.info(f"Attempting to call third-party API: {THIRD_PARTY_API_URL}")
    try:
        response = requests.get(THIRD_PARTY_API_URL, headers=headers, timeout=15)
        response.raise_for_status()
        api_data = response.json()
        log.info(f"Successfully called third-party API. Response data: {api_data}")

    except requests.exceptions.RequestException as e:
        log.error(f"Error calling third-party API: {e}")
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred during third-party API call: {e}")
        raise

    # --- 3. Print IP Addresses ---
    # Get the IP address of the current container/host
    container_ip = "N/A"
    try:
        container_ip = socket.gethostbyname(socket.gethostname())
        log.info(f"Airflow container/worker IP address: {container_ip}")
    except socket.gaierror as e:
        log.warning(f"Could not determine container IP address: {e}")
    except Exception as e:
        log.warning(f"An unexpected error occurred while determining container IP: {e}")


    # Get the destination IP address of the third-party API
    destination_ip = "N/A"
    try:
        parsed_url = urlparse(THIRD_PARTY_API_URL)
        hostname = parsed_url.hostname
        if hostname:
            destination_ip = socket.gethostbyname(hostname)
            log.info(f"Destination IP address ({hostname}): {destination_ip}")
        else:
            log.warning(f"Could not parse hostname from URL: {THIRD_PARTY_API_URL}")
    except socket.gaierror as e:
        log.warning(f"Could not resolve destination IP address for {THIRD_PARTY_API_URL}: {e}")
    except Exception as e:
        log.warning(f"An unexpected error occurred while resolving destination IP: {e}")

    log.info(f"Summary: Container IP: {container_ip}, Destination IP: {destination_ip}")


# --- DAG Definition ---
with DAG(
    dag_id="third_party_api_caller_single_task", # Changed DAG ID to reflect single task
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["api", "external", "example", "single-task"],
    doc_md="""
    ### Third Party API Caller DAG (Single Task)
    This DAG demonstrates how to:
    1. Get an authentication token from a token API.
    2. Use the token to call a third-party API.
    3. Print the IP addresses of the Airflow worker and the destination API.
    All steps are combined into a single Python task.
    **Note**: Replace placeholder URLs and credentials with your actual values.
    """,
) as dag:
    complete_workflow = PythonOperator(
        task_id="complete_api_workflow",
        python_callable=complete_api_workflow_task,
        provide_context=True,
    )
