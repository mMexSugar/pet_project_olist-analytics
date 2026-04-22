from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateTableOperator
from datetime import datetime, timedelta


TABLES = [
    'customers', 'geolocation', 'order_items', 'order_payments', 
    'order_reviews', 'orders', 'products', 'sellers'
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
    
    setup_beam_env = BashOperator(
        task_id='setup_beam_env',
        bash_command=f"python3 -m pip install --user apache-beam[gcp]==2.60.0 pyarrow --quiet"
    )
    
    previous_task = setup_beam_env

    for table in TABLES:
        COMPOSER_SA_EMAIL = "composer-worker-sa@olist-analytics-492311.iam.gserviceaccount.com"
        CUSTOM_IMAGE = "us-east1-docker.pkg.dev/olist-analytics-492311/dataflow-images/dataflow-worker:v1"

        ingest_task = BashOperator(
            task_id=f'ingest_{table}',
            bash_command = f"""
            export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.11/site-packages && \
            gsutil cp {CODE_PATH} ./main_pipeline.py && \

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
                --sdk_container_image {CUSTOM_IMAGE} \
                --service_account_email {COMPOSER_SA_EMAIL} \
                --experiments=enable_preflight_validation=false
            """
        )

        register_bq_task = BigQueryCreateTableOperator(
            task_id=f'register_bq_{table}',
            dataset_id='olist_bronze',
            table_id=table,
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": "olist_bronze",
                    "tableId": table,
                },
                "externalDataConfiguration": {
                    "sourceUris": [f"gs://{PROJECT_ID}-bronze/{table}/*.parquet"],
                    "sourceFormat": "PARQUET",
                    "autodetect": True,
                },
            },
        )

        previous_task >> ingest_task >> register_bq_task
        
        previous_task = register_bq_task