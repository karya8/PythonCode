@provide_session
def create_dags_from_config(config_file="config.json", session=None):
    """Loads config, creates DAG files *only if they don't exist*, and uploads to GCS."""
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading config file: {e}")
        return

    gcs_bucket_name = config['gcs_bucket']
    dags_folder_gcs = "dags"  # Standard dags folder in GCS

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)

    # Get existing files for *comparison* (check directly in GCS)
    existing_blobs = list(bucket.list_blobs(prefix=f"{dags_folder_gcs}/dynamic_dag_"))
    existing_dag_files = {blob.name for blob in existing_blobs if blob.name.endswith(".py")} # Use a set for efficient lookup

    # Get the module name (from the current file)
    module_name = os.path.splitext(os.path.basename(__file__))[0]

    for data_url_config in config['data_urls']:
        dag_id = f"dynamic_dag_{data_url_config['name']}"
        dag_file_name = f"{dag_id}.py"
        blob_path = f"{dags_folder_gcs}/{dag_file_name}"

        # --- Check if the DAG file already exists in GCS ---
        if blob_path in existing_dag_files:
            print(f"DAG file already exists in GCS, skipping: {blob_path}")
            continue  # Skip to the next data_url_config

        default_args = {
            'owner': config.get('owner', 'airflow'),
            'depends_on_past': config.get('depends_on_past', False),
            'start_date': datetime.fromisoformat(config.get('start_date', '2023-11-01T00:00:00')),
            'email': config.get('email', []),
            'email_on_failure': config.get('email_on_failure', False),
            'email_on_retry': config.get('email_on_retry', False),
            'retries': config.get('retries', 1),
            'retry_delay': timedelta(minutes=config.get('retry_delay_minutes', 5)),
        }

        dag_file_content = DAG_TEMPLATE.format(
            dag_id=dag_id,
            default_args=repr(default_args),
            schedule_interval=repr(config.get('schedule_interval', '@hourly')),
            catchup=config.get('catchup', False),
            tags=config.get('tags', ["data_pipeline"]),
            max_active_runs=config.get('max_active_runs', 1),
            data_url_name=data_url_config["name"],
            config=repr(config),
            data_url_config=repr(data_url_config),
            module_name=module_name,
        )

        # Upload directly to GCS
        blob = bucket.blob(blob_path)
        try:
            blob.upload_from_string(dag_file_content, content_type="text/plain")
            print(f"Uploaded DAG file to: gs://{gcs_bucket_name}/{blob_path}")
        except Exception as e:
            print(f"Error uploading DAG file to GCS: {e}")
            # Don't re-raise; continue to the next DAG
