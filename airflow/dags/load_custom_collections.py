from typing import Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from operator import itemgetter
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
from hydra.core.global_hydra import GlobalHydra
from inspector.label_inspector import Inspector
import boto3
import numpy as np
import wordninja

from create_inlets import (
    CollectionDataset,
)
from create_merged import (
    configure_interesting_score,
    configure_force_normalize,
    configure_nomrmal_name_to_hash,
    read_csv_domains,
    AvatarEmoji,
)
from update_es import (
    connect_to_elasticsearch,
    generate_id,
    collect_ids_mapping,
    apply_operations,
    prepare_full_update,
    UPDATING_FIELDS,
)


@dataclass
class S3Config:
    bucket: str
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
    generator_s3_bucket: str


CONFIG = Config(
    email="apohllo@o2.pl",
    local_prefix="/home/airflow/custom-collections/",
    remote_prefix="file:///home/airflow/custom-collections/",
    date_str=datetime.now().strftime("%Y%m%d"),
    start_date=datetime(2021, 1, 1),
    run_interval=timedelta(days=3),  # TODO ??
    s3=S3Config(
        bucket="mhaltiuk-custom-collection-templates",
        region_name="us-east-1",
    ),
    elasticsearch=ElasticsearchConfig(
        scheme=Variable.get("es_scheme", "http"),
        host=Variable.get("es_host", "localhost"),
        port=int(Variable.get("es_port", "9200")),
        index=Variable.get("es_index", "collection-templates-1"),
        username=Variable.get("es_username", "elastic"),
        password=Variable.get("es_password", "changeme"),
    ),
    generator_s3_bucket="prod-name-generator-namegeneratori-inputss3bucket-c26jqo3twfxy"
)


SUGGESTABLE_DOMAINS = CollectionDataset(f"{CONFIG.remote_prefix}suggestable_domains.csv")
AVATAR_EMOJI = CollectionDataset(f"{CONFIG.remote_prefix}avatars-emojis.csv")
INTERESTING_SCORE_CACHE = CollectionDataset(f"{CONFIG.remote_prefix}interesting-score.rocks")
FORCE_NORMALIZE_CACHE = CollectionDataset(f"{CONFIG.remote_prefix}force-normalize.rocks")
NAME_TO_HASH_CACHE = CollectionDataset(f"{CONFIG.remote_prefix}name-to-hash.rocks")

CUSTOM_COLLECTIONS = CollectionDataset(f"{CONFIG.remote_prefix}custom-collections.jsonl")
CUSTOM_COLLECTIONS_PROCESSED = CollectionDataset(f"{CONFIG.remote_prefix}custom-collections-processed.jsonl")
CUSTOM_COLLECTIONS_UPDATE_OPERATIONS = \
    CollectionDataset(f"{CONFIG.remote_prefix}custom-collections-update-operations.jsonl")

MIN_VALUE = 1e-8
# TODO test these
DEFAULT_COLLECTION_RANK = 1_000_000
DEFAULT_MEMBER_RANK = 10_000_000


def download_s3_file(bucket: str, remote_file: str, local_file: str):
    s3 = boto3.client("s3")
    s3.download_file(bucket, remote_file, local_file)


def tokenize_name(name: str) -> list[str]:
    # TODO substitute with our dictionary from NameGenerator
    return wordninja.split(name)


@dataclass
class Member:
    normalized: str
    tokenized: list[str]


@dataclass
class Collection:
    id: str
    name: str
    members: list[Member]
    description: str
    keywords: list[str]
    banner_image: str
    avatar_emoji: str


def generate_random_banner_image():
    banner_image_number = random.randint(0, 19)
    return f'tc-{banner_image_number:02d}.png'


def prepare_custom_collection(
        collection_json: dict,
        inspector: Inspector,
        domains: dict[str, str],
        interesting_score_function: Any,
        force_normalize_function: Any,
        normal_name_to_hash_function: Any,
        avatar_emoji: AvatarEmoji,
) -> dict:

    current_time = time.time() * 1000

    collection_data = collection_json['data']
    commands = collection_json['commands']

    members = []
    for member_json in collection_data['labels']:
        if "normalized_label" not in member_json and "tokenized_label" not in member_json:
            print(f"Skipping member {member_json['label']} because it has no normalized_label and no tokenized_label")
            continue

        if "normalized_label" not in member_json:
            member_json["normalized_label"] = ''.join(member_json["tokenized_label"])
        if "tokenized_label" not in member_json:
            member_json["tokenized_label"] = tokenize_name(member_json["normalized_label"])

        member = Member(normalized=member_json['normalized_label'],
                        tokenized=member_json['tokenized_label'])
        members.append(member)

    collection = Collection(
        id=collection_data['collection_id'],
        name=collection_data['collection_name'],
        members=members,
        description=collection_data.get('collection_description', 'Manually created custom collection'),
        keywords=collection_data.get('collection_keywords', []),
        banner_image=collection_data.get('banner_image', generate_random_banner_image()),
        avatar_emoji=collection_data.get('avatar_emoji', None),
    )

    template_names = [{
        'normalized_name': member.normalized,
        'tokenized_name': member.tokenized,
        'system_interesting_score': interesting_score_function(member.normalized)[0],
        'rank': commands.get('member_rank', DEFAULT_MEMBER_RANK),
        'cached_status': domains.get(member.normalized, None),
        'namehash': normal_name_to_hash_function(member.normalized + '.eth'),
        # 'translations_count': None,
    } for member in collection.members]

    if commands.get('sort_labels', 'none') == 'interesting_score':
        template_names.sort(key=itemgetter('system_interesting_score'), reverse=True)
    elif commands.get('sort_labels', 'none') == 'shortest':
        template_names.sort(key=lambda name: len(name['tokenized_name']))
    elif commands.get('sort_labels', 'none') == 'longest':
        template_names.sort(key=lambda name: len(name['tokenized_name']), reverse=True)
    elif commands.get('sort_labels', 'none') == 'a-z':
        template_names.sort(key=itemgetter('normalized_name'))
    elif commands.get('sort_labels', 'none') == 'z-a':
        template_names.sort(key=itemgetter('normalized_name'), reverse=True)

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
                'normalized_name': name['normalized_name'],
                'avatar_override': '',
                'tokenized_name': name['tokenized_name'],
            } for name in template_names],
            'collection_description': collection.description,
            'collection_keywords': collection.keywords,
            'collection_image': None,  # TODO can there be a custom collection image?
            'public': True,

            'banner_image': collection.banner_image,
            'avatar_image': None,
            'avatar_emoji': avatar_emoji.get_emoji(collection.id, []),

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
            'collection_wikipedia_link': None,
            'collection_wikidata_id': None,
            'collection_types': [],
            'collection_rank': commands.get('collection_rank', DEFAULT_COLLECTION_RANK),
            'translations_count': None,
            'has_related': None,

            'collection_images': None,
            'collection_page_banners': None,

            'names': template_names,
            'top10_names': template_names[:10],
            'top25_names': template_names[:25],

            # below metrics calculated on members
            'members_rank_mean': max(np.mean(ranks), MIN_VALUE),
            'members_rank_median': max(np.median(ranks), MIN_VALUE),
            'members_system_interesting_score_mean': max(np.mean(interesting_scores), MIN_VALUE),
            'members_system_interesting_score_median': max(np.median(interesting_scores), MIN_VALUE),
            'valid_members_count': len(collection.members),
            'invalid_members_count': 1,  # rank features cannot be zero  # TODO ??
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
    GlobalHydra.instance().clear()
    initialize_config_module(version_base=None, config_module='inspector_conf')
    config = compose(config_name="prod_config")
    inspector = Inspector(config)

    domains = read_csv_domains(domains_path)
    interesting_score_function = configure_interesting_score(inspector, interesting_score_path)
    force_normalize_function = configure_force_normalize(force_normalize_path)
    normal_name_to_hash_function = configure_nomrmal_name_to_hash(name_to_hash_path)

    avatar_emoji = AvatarEmoji(avatar_emoji_path)  # FIXME how do we get type?

    with jsonlines.open(input_file, 'r') as reader, jsonlines.open(output_file, 'w') as writer:
        for collection_json in reader:
            prepared_collection = prepare_custom_collection(
                collection_json=collection_json,
                inspector=inspector,
                domains=domains,
                interesting_score_function=interesting_score_function,
                force_normalize_function=force_normalize_function,
                normal_name_to_hash_function=normal_name_to_hash_function,
                avatar_emoji=avatar_emoji,
            )
            writer.write(prepared_collection)


def produce_custom_update_operations(custom_collections_path: str, custom_update_operations: str):
    es = connect_to_elasticsearch(CONFIG.elasticsearch)
    ids_mapping = collect_ids_mapping(es, CONFIG.elasticsearch.index)

    ops = custom_update_operations
    with jsonlines.open(custom_collections_path, 'r') as reader, jsonlines.open(ops, 'w') as writer:
        for collection in reader:
            metadata_id = collection['metadata']['id']

            if metadata_id in ids_mapping:
                # there are not that many custom collections, thus no need for optimization - executing a full update
                writer.write(prepare_full_update(
                    ids_mapping[metadata_id],
                    collection,
                    UPDATING_FIELDS,  # TODO this may change eventually
                    CONFIG.elasticsearch.index
                ))
            else:
                writer.write({
                    '_index': CONFIG.elasticsearch.index,
                    '_op_type': 'create',
                    '_id': generate_id(),
                    '_source': collection
                })


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
        # schedule=[Dataset(CONFIG.remote_prefix)],  # TODO update path
        start_date=CONFIG.start_date,
        catchup=False,
        tags=["load-custom-collections", "custom-collections"],
) as dag:
    download_custom_collections_task = PythonOperator(
        task_id='download-custom-collections',
        python_callable=download_s3_file,
        op_kwargs={
            "bucket": CONFIG.s3.bucket,
            "remote_file": CUSTOM_COLLECTIONS.name(),
            "local_file": CUSTOM_COLLECTIONS.local_name()
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
        python_callable=download_s3_file,
        op_kwargs={
            "bucket": CONFIG.generator_s3_bucket,
            "remote_file": SUGGESTABLE_DOMAINS.name(),
            "local_file": SUGGESTABLE_DOMAINS.local_name()
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
        python_callable=download_s3_file,
        op_kwargs={
            "bucket": CONFIG.generator_s3_bucket,
            "remote_file": AVATAR_EMOJI.name(),
            "local_file": AVATAR_EMOJI.local_name()
        },
    )

    prepare_custom_collections_task = PythonOperator(
        task_id='prepare-custom-collections',
        python_callable=prepare_custom_collections,
        op_kwargs={
            "input_file": CUSTOM_COLLECTIONS.local_name(),
            "output_file": CUSTOM_COLLECTIONS_PROCESSED.local_name(),
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

    produce_update_operations_task = PythonOperator(
        task_id='produce-update-operations',
        python_callable=produce_custom_update_operations,
        op_kwargs={
            "custom_collections_path": CUSTOM_COLLECTIONS_PROCESSED.local_name(),
            "custom_update_operations": CUSTOM_COLLECTIONS_UPDATE_OPERATIONS.local_name()
        }
    )
    produce_update_operations_task.doc_md = dedent(
        """\
    #### Task Documentation
    Produce Elasticsearch update operations of the custom collections
    """
    )

    update_elasticsearch_task = PythonOperator(
        task_id='update-elasticsearch-custom-collections',
        python_callable=apply_operations,
        op_kwargs={
            "operations": CUSTOM_COLLECTIONS_UPDATE_OPERATIONS.local_name()
        }
    )
    update_elasticsearch_task.doc_md = dedent(
        """\
    #### Task Documentation
    Update Elasticsearch index with the newly created and updated custom collections by applying produced operations
    """
    )

    [download_custom_collections_task, download_suggestable_domains_task, download_avatars_emojis_task] \
        >> prepare_custom_collections_task >> produce_update_operations_task >> update_elasticsearch_task
