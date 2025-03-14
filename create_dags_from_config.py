@provide_session
def create_dags_from_config(config_file="config.json", session=None):
    """Loads config and creates DAGs."""
    print(f"Loading config from: {config_file}")
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        print(f"Loaded config: {config}")  # Print the entire config
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading config file: {e}")
        return

    dagbag = DagBag(dag_folder=conf.get('core', 'dags_folder'), include_examples=False)
    print(f"DagBag: {dagbag.dags}")  # Print existing DAGs

    for data_url_config in config['data_urls']:
        dag_id = f"dynamic_dag_{data_url_config['name']}"
        print(f"Checking for DAG: {dag_id}")
        if dag_id not in dagbag.dags:
            print(f"Creating DAG: {dag_id}")
            dag = create_dag(dag_id, config, data_url_config)
            globals()[dag_id] = dag
            print(f"DAG created and added to globals: {dag_id}")  # Confirmation
        else:
            print(f"DAG {dag_id} already exists.")

def create_dag(dag_id, config, data_url_config):
    """Creates a dynamic DAG based on the configuration."""
    print(f"create_dag called with dag_id: {dag_id}, config: {config}, data_url_config: {data_url_config}") # Debug print

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
    print(f"default_args: {default_args}")  # Print default_args


    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=config.get('schedule_interval', '@hourly'),
        catchup=config.get('catchup', False),
        tags=config.get('tags', ["data_pipeline"]),
        max_active_runs=config.get('max_active_runs', 1)
    )

    with dag:
        start = DummyOperator(task_id='start')
        end = DummyOperator(task_id='end')

        process_task = PythonOperator(
            task_id=f'process_{data_url_config["name"]}',
            python_callable=process_data_url,
            op_kwargs={
                'config': config,
                'data_url_config': data_url_config,
                'current_time': datetime.utcnow()
            },
        )
        start >> process_task >> end

    return dag
