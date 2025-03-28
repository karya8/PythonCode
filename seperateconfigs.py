@provide_session
def create_dags_from_config(config_file="config.json", session=None):
    """Loads config, creates DAG files *only if they don't exist*, uploads to GCS."""
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading main config file: {e}")
        return

    gcs_bucket_name = os.environ.get("GCS_BUCKET")
    if not gcs_bucket_name:
        print("ERROR: GCS_BUCKET environment variable not set.")
        return

    dags_folder_gcs = "dags"
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)

    dags_folder = conf.get('core', 'dags_folder')
    combined_configs_dir = os.path.join(dags_folder, config['combined_configs_dir'])
    os.makedirs(combined_configs_dir, exist_ok=True)

    existing_blobs = list(bucket.list_blobs(prefix=f"{dags_folder_gcs}/dynamic_dag_"))
    existing_dag_files = {blob.name for blob in existing_blobs if blob.name.endswith(".py")}
    module_name = os.path.splitext(os.path.basename(__file__))[0]

    # --- Iterate through combined config files, with filtering ---
    for config_file_name in os.listdir(combined_configs_dir):
        # Skip the master config file and files from other projects
        if (not config_file_name.startswith("our_project_") or  # Naming convention
                not config_file_name.endswith(".json") or
                config_file_name == os.path.basename(config_file)):  # Exclude master config
            continue

        config_file_path = os.path.join(combined_configs_dir, config_file_name)
        try:
            with open(config_file_path, "r") as f:
                combined_config = json.load(f)
                data_url_config = combined_config['data_url_config']
                dag_config = combined_config['dag_config']
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            print(f"Error loading/processing {config_file_path}: {e}")
            continue

        dag_id = f"dynamic_dag_{data_url_config['name']}"
        dag_file_name = f"{dag_id}.py"
        blob_path = f"{dags_folder_gcs}/{dag_file_name}"

        if blob_path in existing_dag_files:
            print(f"DAG file already exists in GCS, skipping: {blob_path}")
            continue

        schedule_interval = dag_config.get('schedule_interval')
        schedule_interval_str = 'None' if schedule_interval is None else repr(schedule_interval)

        start_date_str = format_datetime_for_code(
            datetime.fromisoformat(dag_config.get('start_date', '2023-11-01T00:00:00'))
        )
        retry_delay_str = format_timedelta_for_code(
            timedelta(minutes=dag_config.get('retry_delay_minutes', 5))
        )

        dag_file_content = DAG_TEMPLATE.format(
            dag_id=dag_id,
            schedule_interval=schedule_interval_str,
            catchup=dag_config.get('catchup', False),
            tags=dag_config.get('tags', ["data_pipeline"]),
            max_active_runs=dag_config.get('max_active_runs', 1),
            data_url_name=data_url_config["name"],
            config=json.dumps(config, indent=4),
            data_url_config=json.dumps(data_url_config, indent=4),
            module_name=module_name,
            start_date=start_date_str,
            owner=repr(dag_config.get('owner', 'airflow')),
            depends_on_past=dag_config.get('depends_on_past', False),
            retries=dag_config.get('retries', 1),
            retry_delay=retry_delay_str,
        )

        blob = bucket.blob(blob_path)
        try:
            blob.upload_from_string(dag_file_content, content_type="text/plain")
            print(f"Uploaded DAG file to: gs://{gcs_bucket_name}/{blob_path}")
        except Exception as e:
            print(f"Error uploading DAG file to GCS: {e}")
