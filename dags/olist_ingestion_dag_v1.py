from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowRunPipelineOperator
from datetime import datetime, timedelta


TABLES = [
    'customers', 'geolocation', 'order_items', 'order_payments', 
    'order_reviews', 'orders', 'products', 'sellers', 'category_translation'
]

PROJECT_ID = "olist-analytics-492311"
REGION = "us-central1"
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
    schedule_interval=None,
    catchup=False,
    tags=['olist', 'bronze'],
) as dag:
    
    previous_task = None

    for table in TABLES:
        COMPOSER_SA_EMAIL = "composer-worker-sa@olist-analytics-492311.iam.gserviceaccount.com"

        gcloud_command = f"""
        export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.11/site-packages && \
        gsutil cp {CODE_PATH} ./main_pipeline.py && \
        gsutil cp gs://{PROJECT_ID}-code/requirements.txt ./requirements.txt && \
        python3 -m pip install --user apache-beam[gcp] pyarrow --quiet && \
        python3 ./main_pipeline.py \
            --runner DataflowRunner \
            --worker_machine_type e2-standard-2 \
            --project {PROJECT_ID} \
            --region {REGION} \
            --table {table} \
            --input gs://{PROJECT_ID}-landing/olist_{table}_dataset.csv \
            --output gs://{PROJECT_ID}-bronze/{table}/{table} \
            --staging_location gs://{PROJECT_ID}-landing/staging \
            --temp_location gs://{PROJECT_ID}-landing/temp \
            --requirements_file ./requirements.txt \
            --service_account_email {COMPOSER_SA_EMAIL}
        """

        task = BashOperator(
            task_id=f'ingest_{table}',
            bash_command=gcloud_command
        )

        if previous_task:
            previous_task >> task
            
        previous_task = task