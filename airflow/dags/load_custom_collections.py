from typing import Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from textwrap import dedent
import jsonlines
import random
import time
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
import numpy as np

from create_inlets import (
    download_suggestable_domains,
    CollectionDataset,
)
from create_merged import (
    configure_interesting_score,
    configure_force_normalize,
    configure_nomrmal_name_to_hash,
    read_csv_domains,
    AvatarEmoji,
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
    remote_prefix: str
    date_str: str
    start_date: datetime
    run_interval: timedelta
    s3: S3Config
    elasticsearch: ElasticsearchConfig


CONFIG = Config(
    email="apohllo@o2.pl",
    local_prefix="/home/airflow/custom-collections/",
    remote_prefix="file:///home/airflow/custom-collections/",
    date_str=datetime.now().strftime("%Y%m%d"),
    start_date=datetime(2021, 1, 1),
    run_interval=timedelta(days=3),  # TODO ??
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


SUGGESTABLE_DOMAINS = CollectionDataset(f"{CONFIG.remote_prefix}suggestable_domains.csv")
AVATAR_EMOJI = CollectionDataset(f"{CONFIG.remote_prefix}avatars-emojis.csv")
INTERESTING_SCORE_CACHE = CollectionDataset(f"{CONFIG.remote_prefix}interesting-score.rocks")
FORCE_NORMALIZE_CACHE = CollectionDataset(f"{CONFIG.remote_prefix}force-normalize.rocks")
NAME_TO_HASH_CACHE = CollectionDataset(f"{CONFIG.remote_prefix}name-to-hash.rocks")

MIN_VALUE = 1e-8


def download_s3_file(bucket: str, remote_file: str, local_file: str):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=CONFIG.s3.access_key_id,
        aws_secret_access_key=CONFIG.s3.secret_access_key,
        region_name=CONFIG.s3.region_name
    )
    s3.download_file(bucket, remote_file, local_file)


@dataclass
class Member:
    tokenized: list[str]  # FIXME what if there is no tokenized name? how do we spot it? do we use our tokenizer then?

    @property
    def curated(self) -> str:
        return ''.join(self.tokenized)


@dataclass
class Collection:
    id: str
    name: str
    members: list[Member]
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


def prepare_custom_collection(
        collection: dict,
        domains_path: str,
        interesting_score_path: str,
        force_normalize_path: str,
        name_to_hash_path: str,
        avatar_emoji_path: str,
) -> dict:

    initialize_config_module(version_base=None, config_module='inspector_conf')
    config = compose(config_name="prod_config")
    inspector = Inspector(config)

    domains = read_csv_domains(domains_path)
    interesting_score_function = configure_interesting_score(inspector, interesting_score_path)
    force_normalize_function = configure_force_normalize(force_normalize_path)
    normal_name_to_hash_function = configure_nomrmal_name_to_hash(name_to_hash_path)

    avatar_emoji = AvatarEmoji(avatar_emoji_path)  # FIXME how do we get type?
    current_time = time.time() * 1000

    collection = Collection(**collection)

    template_names = [{
        'normalized_name': member.curated,
        'tokenized_name': member.tokenized,
        'system_interesting_score': interesting_score_function(member.curated)[0],
        'rank': ...,  # TODO what do we do with this?
        'cached_status': domains.get(member.curated, None),
        'namehash': normal_name_to_hash_function(member.curated + '.eth'),
        # 'translations_count': None,
    } for member in collection.members]

    ranks = [name['rank'] for name in template_names]
    interesting_scores = [name['system_interesting_score'] for name in template_names]
    status_counts = dict.fromkeys(['available', 'taken', 'on_sale', 'recently_released', 'never_registered'], 0)
    for name in template_names:
        status = name['cached_status']
        if status is None:
            status = 'never_registered'
        status_counts[status] += 1

    nonavailable_members = status_counts['taken'] + status_counts['on_sale'] + status_counts['recently_released']

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
            'collection_image': None,  # TODO can there be a custom collection image?
            'public': True,

            'banner_image': collection.banner_image,
            'avatar_image': None,
            'avatar_emoji': avatar_emoji.get_emoji(collection.id, ...),

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
            'collection_name_log_probability': inspector.ngrams.sequence_log_probability(
                collection.name.lower().split(' ')
            ),
        },
        'template': {
            'collection_wikipedia_link': ...,  # TODO
            'collection_wikidata_id': ...,  # TODO
            'collection_types': ...,  # TODO
            'collection_rank': ...,  # TODO
            'translations_count': None,
            'has_related': None,

            'collection_images': collection.image,  # TODO ??
            'collection_page_banners': collection.page_banner,  # TODO ??

            'names': template_names,
            'top10_names': template_names[:10],
            'top25_names': template_names[:25],

            # below metrics calculated on members
            'members_rank_mean': max(np.mean(ranks), MIN_VALUE),
            'members_rank_median': max(np.median(ranks), MIN_VALUE),
            'members_system_interesting_score_mean': max(np.mean(interesting_scores), MIN_VALUE),
            'members_system_interesting_score_median': max(np.median(interesting_scores), MIN_VALUE),
            'valid_members_count': len(collection.members),
            'invalid_members_count': 0,  # TODO ??
            'valid_members_ratio': 1.0,  # TODO ??
            'nonavailable_members_count': nonavailable_members,
            'nonavailable_members_ratio': max(nonavailable_members / len(collection.members), MIN_VALUE),

            'is_merged': False,
            'available_count': status_counts['available'],
            'taken_count': status_counts['taken'],
            'on_sale_count': status_counts['on_sale'],
            'recently_released_count': status_counts['recently_released'],
            'never_registered_count': status_counts['never_registered'],
        },
        'name_generator': {

        },
    }

    return collection_json


def prepare_custom_collections(
        input_file: str,
        output_file: str,
        domains_path: str,
        interesting_score_path: str,
        force_normalize_path: str,
        name_to_hash_path: str,
        avatar_emoji_path: str,
):
    with jsonlines.open(input_file, 'r') as reader, jsonlines.open(output_file, 'w') as writer:
        for collection in reader:
            prepared_collection = prepare_custom_collection(
                collection=collection,
                domains_path=domains_path,
                interesting_score_path=interesting_score_path,
                force_normalize_path=force_normalize_path,
                name_to_hash_path=name_to_hash_path,
                avatar_emoji_path=avatar_emoji_path,
            )
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

    download_suggestable_domains_task = PythonOperator(
        outlets=[SUGGESTABLE_DOMAINS],
        task_id="download-suggestable-domains",
        python_callable=download_suggestable_domains,
        op_kwargs={
            "bucket": "prod-name-generator-namegeneratori-inputss3bucket-c26jqo3twfxy",
            "name": SUGGESTABLE_DOMAINS.name(),
            "path": SUGGESTABLE_DOMAINS.local_name()
        },
    )
    download_suggestable_domains_task.doc_md = dedent(
        """\
    #### Task Documentation
    Download CSV file with suggestable domains from S3
    """
    )

    download_avatars_emojis_task = PythonOperator(
        outlets=[AVATAR_EMOJI],
        task_id="download-avatars-emojis",
        python_callable=download_suggestable_domains,
        op_kwargs={
            "bucket": "prod-name-generator-namegeneratori-inputss3bucket-c26jqo3twfxy",
            "name": AVATAR_EMOJI.name(),
            "path": AVATAR_EMOJI.local_name()
        },
    )

    prepare_custom_collections_task = PythonOperator(
        task_id='prepare-custom-collections',
        python_callable=prepare_custom_collections,
        op_kwargs={
            "input_file": 'custom-collections.jsonl',
            "output_file": 'custom-collections-processed.jsonl',
            "domains_path": SUGGESTABLE_DOMAINS.local_name(),
            "interesting_score_path": INTERESTING_SCORE_CACHE.local_name(),
            "force_normalize_path": FORCE_NORMALIZE_CACHE.local_name(),
            "name_to_hash_path": NAME_TO_HASH_CACHE.local_name(),
            "avatar_emoji_path": AVATAR_EMOJI.local_name(),
        },
    )
    prepare_custom_collections_task.doc_md = dedent(
        """\
    #### Task Documentation
    Process JSONL file with new custom collections to the proper format
    """
    )

    [download_custom_collections_task, download_suggestable_domains_task, download_avatars_emojis_task] \
        >> prepare_custom_collections_task
