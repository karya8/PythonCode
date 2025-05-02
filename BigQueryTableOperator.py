# Task to create (or replace) the external BigQuery table
    create_external_table_task = BigQueryTableOperator(
        task_id="create_external_table",
        task_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",  # Use task_table
        table_resource={
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
            "externalDataConfiguration": {
                "sourceFormat": SOURCE_FORMAT,
                "sourceUris": SOURCE_URIS,
                "csvOptions": CSV_OPTIONS,
            },
            "schema": {"fields": bq_schema},
            **(table_metadata if table_metadata else {}),  # Include table metadata if available
        },
        gcp_conn_id=gcp_conn_id,
        create_disposition="CREATE_IF_NEEDED",  # Or "CREATE_ALWAYS"
        write_disposition="WRITE_TRUNCATE", # Or WRITE_APPEND, WRITE_EMPTY
    )
