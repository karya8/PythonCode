def process_zip_files(**kwargs):
    """Processes zip files from a GCS bucket, using landing, error, schema, zip, and csv folders."""
    config = load_config()
    bucket_name = config['bucket_name']
    landing_folder = config['landing_folder']
    error_folder = config['error_folder']
    schema_folder = config['schema_folder']
    zip_folder = config['zip_folder']
    csv_folder = config['csv_folder']
    schema_file_name = config['schema_file']

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=landing_folder + '/')
    zip_blobs = [blob for blob in blobs if blob.name.endswith('.zip')]

    schema_blob = bucket.blob(schema_folder + '/' + schema_file_name)
    schema_content = schema_blob.download_as_text()
    schema_reader = csv.reader(io.StringIO(schema_content))
    try:
        schema_header = next(schema_reader)
    except StopIteration:
        print(f"Error: Schema file {schema_file_name} is empty.")
        return

    for zip_blob in zip_blobs:
        try:
            zip_content = zip_blob.download_as_bytes()
            with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zip_ref:
                csv_file_name = [name for name in zip_ref.namelist() if name.endswith('.csv')][0]
                with zip_ref.open(csv_file_name) as csv_file:
                    csv_content = csv_file.read().decode('utf-8')
                    csv_reader = csv.reader(io.StringIO(csv_content))
                    try:
                         csv_header = next(csv_reader)
                    except StopIteration:
                         print(f"Error: CSV file {csv_file_name} in {zip_blob.name} is empty.")
                         raise ValueError(f"CSV file {csv_file_name} in {zip_blob.name} is empty.")

                    if csv_header == schema_header:
                        # Move zip to zip folder
                        destination_blob_name_zip = zip_blob.name.replace(landing_folder, zip_folder, 1)
                        destination_blob_zip = bucket.blob(destination_blob_name_zip)
                        bucket.copy_blob(zip_blob, bucket, destination_blob_name_zip)
                        zip_blob.delete()

                        # Move csv to csv folder
                        csv_blob_name = os.path.splitext(zip_blob.name)[0].replace(landing_folder,csv_folder,1) + ".csv"
                        csv_blob_e = bucket.blob(csv_blob_name)
                        csv_blob_e.upload_from_string(csv_content)

                    else:
                        # Move zip to error folder
                        destination_blob_name_error = zip_blob.name.replace(landing_folder, error_folder, 1)
                        destination_blob_error = bucket.blob(destination_blob_name_error)
                        bucket.copy_blob(zip_blob, bucket, destination_blob_name_error)
                        zip_blob.delete()

        except Exception as e:
            print(f"Error processing {zip_blob.name}: {e}")
            destination_blob_name_error = zip_blob.name.replace(landing_folder, error_folder, 1)
            destination_blob_error = bucket.blob(destination_blob_name_error)
            bucket.copy_blob(zip_blob, bucket, destination_blob_name_error)
            zip_blob.delete()

# The rest of the DAG definition remains the same
with DAG(
    dag_id='process_zip_files_dag',
    schedule_interval=None,  # Run manually or adjust schedule
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
) as dag:
    process_task = PythonOperator(
        task_id='process_zip_files',
        python_callable=process_zip_files,
        provide_context=True,
    )

