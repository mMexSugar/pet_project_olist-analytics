from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from datetime import datetime, timedelta

# Список твоих таблиц
TABLES = [
    'customers', 'geolocation', 'order_items', 'order_payments', 
    'order_reviews', 'orders', 'products', 'sellers', 'category_translation'
]

PROJECT_ID = "olist-analytics-492311"
REGION = "us-central1"
BUCKET_NAME = f"{PROJECT_ID}-landing"
CODE_PATH = f"gs://{PROJECT_ID}-code/main_pipeline.py"

default_args = {
    'owner': 'Maksym',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'olist_bronze_ingestion',
    default_args=default_args,
    schedule_interval=None, # Запускаем вручную
    catchup=False,
    tags=['olist', 'bronze'],
) as dag:

    for table in TABLES:
        process_table = BeamRunPythonPipelineOperator(
            task_id=f'ingest_{table}',
            py_file=CODE_PATH,
            pipeline_options={
                'table': table,
                'input': f'gs://{PROJECT_ID}-landing/olist_{table}_dataset.csv',
                'output': f'gs://{PROJECT_ID}-bronze/{table}/{table}',
                'region': REGION,
                'project': PROJECT_ID,
                'temp_location': f'gs://{PROJECT_ID}-landing/temp',
                'requirements_file': f"gs://{PROJECT_ID}-code/requirements.txt",
            },
            # Настройки инфраструктуры Dataflow
            py_options=[],
            py_interpreter='python3',
            py_system_site_packages=False,
            runner='DataflowRunner',
            dataflow_config=DataflowConfiguration(
                job_name=f'olist-ingest-{table.replace("_", "-")}',
                project_id=PROJECT_ID,
                location=REGION,
                wait_until_finished=True,
            )
        )