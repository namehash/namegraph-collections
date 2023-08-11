from datetime import datetime, timedelta
from textwrap import dedent
import re
import boto3

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, Dataset

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def download_from_s3(bucket, name, path):
    s3 = boto3.client('s3')
    s3.download_file(bucket, name, path)

def upload_to_s3(bucket, name, path):
    s3 = boto3.client('s3')
    s3.upload_file(path, bucket, name)

with DAG(
    "test-download",
    default_args={
        "email": "nobody@nowhere.com",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": "/home/airflow/data/",
    },
    description="Test task related to downloading data from S3",
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["test", "download", "upload", "head"],
) as dag:
    download_task = PythonOperator(
        outlets=[],
        task_id="download-task",
        python_callable=download_from_s3,

        op_kwargs={
            "bucket": "prod-name-generator-namegeneratori-inputss3bucket-c26jqo3twfxy",
            "name": "avatars-emojis.csv", 
            "path": "avatars-emojis.csv",
        },
    )

    head_task = BashOperator(
        task_id="head-task",
        bash_command=f"head avatars-emojis.csv > avatars-emojis-head.csv",
    )

    upload_task = PythonOperator(
        outlets=[],
        task_id="upload-task",
        python_callable=upload_to_s3,
        op_kwargs={
            "bucket": "prod-name-generator-namegeneratori-inputss3bucket-c26jqo3twfxy",
            "name": "avatars-emojis-head.csv", 
            "path": "/home/airflow/data/" + "avatars-emojis-head.csv",
        },
    )

    download_task >> head_task >> upload_task

