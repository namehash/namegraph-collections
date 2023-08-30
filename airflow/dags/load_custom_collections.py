from typing import Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from textwrap import dedent
import jsonlines
import random
import os

from airflow import DAG
from airflow.models import Variable
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan
from hydra import initialize_config_module, compose
from inspector.label_inspector import Inspector
import boto3

from create_merged import (
    configure_interesting_score,
    configure_force_normalize,
    read_csv_domains
)
from update_es import connect_to_elasticsearch


@dataclass
class S3Config:
    bucket: str
    access_key_id: str
    secret_access_key: str
    region_name: str


@dataclass
class ElasticsearchConfig:
    scheme: str
    host: str
    port: int
    index: str
    username: str
    password: str


@dataclass
class Config:
    email: str
    local_prefix: str
    remote_file: str
    date_str: str
    start_date: datetime
    s3: S3Config
    elasticsearch: ElasticsearchConfig


CONFIG = Config(
    email="apohllo@o2.pl",
    local_prefix="/home/airflow/data/custom-collections/",
    date_str=datetime.now().strftime("%Y%m%d"),
    start_date=datetime(2021, 1, 1),
    s3=S3Config(
        bucket="mhaltiuk-custom-collection-templates",
        access_key_id=Variable.get("s3_access_key_id", "minio"),
        secret_access_key=Variable.get("s3_secret_access_key", "minio123"),
        region_name="us-east-1",
    ),
    elasticsearch=ElasticsearchConfig(
        scheme=Variable.get("es_scheme", "http"),
        host=Variable.get("es_host", "localhost"),
        port=int(Variable.get("es_port", "9200")),
        index=Variable.get("es_index", "collection-templates-1"),
        username=Variable.get("es_username", "elastic"),
        password=Variable.get("es_password", "changeme"),
    )
)


def download_s3_file(bucket: str, remote_file: str, local_file: str):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=CONFIG.s3.access_key_id,
        aws_secret_access_key=CONFIG.s3.secret_access_key,
        region_name=CONFIG.s3.region_name
    )
    s3.download_file(bucket, remote_file, local_file)


@dataclass
class Collection:
    id: str
    name: str
    members: list[str]
    description: str = field(default='Manually created custom collection')
    keywords: list[str] = field(default_factory=list)
    banner_image: str = field(default=None)
    avatar_emoji: str = field(default=None)

    def __post_init__(self):
        if self.banner_image is None:
            banner_image_number = random.randint(0, 19)
            self.banner_image = f'tc-{banner_image_number:02d}.png'

        if self.avatar_emoji is None:
            pass


def prepare_custom_collection(collection: dict) -> dict:
    initialize_config_module(version_base=None, config_module='inspector_conf')
    config = compose(config_name="prod_config")
    inspector = Inspector(config)

    # domains = read_csv_domains(...)
    # interesting_score_function = configure_interesting_score(inspector, 'interesting_score.cache')
    # force_normalize_function = configure_force_normalize('force_normalize.cache')

    collection = Collection(**collection)

    collection_json = {
        'data': {
            'collection_name': collection.name,
            'names': [{
                'normalized_name': member.curated,
                'avatar_override': '',
                'tokenized_name': member.tokenized,
            } for member in collection.members],
            'collection_description': collection.description,
            'collection_keywords': collection.keywords,
            'collection_image': ...,  # TODO
            'public': True,

            'banner_image': collection.banner_image,
            'avatar_image': None,
            'avatar_emoji': avatar_emoji.get_emoji(collection.item, [type for cid, type in collection.types]),

            'archived': False,
        },
        'curation': {
            'curated': False,
            'category': '',
            'trending': False,
            'community-choice': False,
        },
        'metadata': {
            'id': collection.id,
            'type': 'custom',
            'version': 0,
            'owner': '0xcb8f5f88e997527d76401ce3df8c8542b676e149',
            'created': current_time,
            'modified': current_time,
            'votes': [],
            'duplicated-from': '',
            'members_count': len(collection.members),
            'collection_name_log_probability': inspector.ngrams.sequence_log_probability(...),
        },
        # TODO
    }

    return collection_json


def prepare_custom_collections(input_file: str, output_file: str):
    with jsonlines.open(input_file, 'r') as reader, jsonlines.open(output_file, 'w') as writer:
        for collection in reader:
            prepared_collection = prepare_custom_collection(collection)
            writer.write(prepared_collection)


with DAG(
        "load-custom-collections",
        default_args={
            "email": [CONFIG.email],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "cwd": CONFIG.local_prefix,
            "start_date": CONFIG.start_date,
        },
        description="Loads custom collections from S3, processes them and loads to Elasticsearch",
        schedule=[Dataset(CONFIG.remote_file)],
        start_date=CONFIG.start_date,
        catchup=False,
        tags=["load-custom-collections", "custom-collections"],
) as dag:
    download_custom_collections_task = PythonOperator(
        task_id='download-custom-collections',
        python_callable=download_s3_file,
        op_kwargs={
            "bucket": CONFIG.s3.bucket,
            "remote_file": 'custom-collections.jsonl',
            "local_file": 'custom-collections.jsonl',
        },
    )
    download_custom_collections_task.doc_md = dedent(
        """\
    #### Task Documentation
    Download JSONL file with new custom collections from S3
    """
    )

    prepare_custom_collections_task = PythonOperator(
        task_id='prepare-custom-collections',
        python_callable=prepare_custom_collections,
        op_kwargs={
            "input_file": 'custom-collections.jsonl',
            "output_file": 'custom-collections-processed.jsonl',
        },
    )
    prepare_custom_collections_task.doc_md = dedent(
        """\
    #### Task Documentation
    Process JSONL file with new custom collections to the proper format
    """
    )
