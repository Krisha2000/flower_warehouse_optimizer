from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='flower_warehouse_optimizer_dag',
    start_date=datetime(2023, 1, 1),  # Start date in the past
    schedule_interval=timedelta(seconds=10),  # Run every 10 seconds
    catchup=False,
    doc_md="""
    ### Flower Warehouse Optimization Pipeline
    This DAG runs a full ELT-ETL pipeline to manage a flower warehouse.
    - **db_init**: Sets up the database schema (should be run manually once).
    - **run_pipeline**: Ingests new data and runs storage/fulfillment optimizations.
    """
) as dag:
    
    # This task should be run manually only the very first time to create your tables.
    init_db_task = BashOperator(
        task_id='initialize_database_schema',
        bash_command='python /opt/airflow/scripts/db_init.py'
    )

    # This is the main task that will run every 10 seconds.
    run_pipeline_task = BashOperator(
        task_id='run_optimization_pipeline',
        bash_command='python /opt/airflow/scripts/run_pipeline.py'
    )

    # Ensure that database initialization happens before pipeline runs (only on first run)
    init_db_task >> run_pipeline_task
