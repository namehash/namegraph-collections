from dataclasses import dataclass
from datetime import datetime
import jsonlines
import requests
import tqdm

from textwrap import dedent
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from elasticsearch import Elasticsearch, ConflictError
from elasticsearch.helpers import streaming_bulk, scan

from create_inlets import CollectionDataset


@dataclass
class AWSConfig:
    access_key_id: str
    secret_access_key: str


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
    github_token: str
    aws: AWSConfig
    elasticsearch: ElasticsearchConfig


CONFIG = Config(
    email="apohllo@o2.pl",
    local_prefix="/home/airflow/precomputing-related-collections/",
    remote_prefix="file:///home/airflow/precomputing-related-collections/",
    date_str=datetime.now().strftime("%Y%m%d"),
    start_date=datetime(2021, 1, 1),
    github_token=Variable.get("github_token"),
    aws=AWSConfig(
        access_key_id=Variable.get("s3_access_key_id", "minio"),
        secret_access_key=Variable.get("s3_secret_access_key", "minio123"),
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


PRECOMPUTED_RELATED_COLLECTIONS = CollectionDataset(
    f"{CONFIG.remote_prefix}precomputed-related-collections.jsonl"
)


def connect_to_elasticsearch(config: ElasticsearchConfig):
    return Elasticsearch(
        hosts=[{
            'scheme': config.scheme,
            'host': config.host,
            'port': config.port
        }],
        http_auth=(config.username, config.password),
        timeout=60,
        http_compress=True,
    )


def collect_existing_collections(es: Elasticsearch, index: str) -> list[dict[str, str]]:
    query = {
        'query': {
            'match_all': {}
        },
        '_source': ['metadata.id', 'data.collection_name'],
    }

    collections = []
    for doc in tqdm.tqdm(
            scan(es, index=index, query=query),
            total=es.count(index=index)['count'],
            desc='collecting existing collections'
    ):
        collections.append({
            'id': doc['_id'],
            'wikidata_id': doc['_source']['metadata']['id'],
            'name': doc['_source']['data']['collection_name'],
        })

    return collections


def generate_related_collections(url: str, collection_id: str, related_num: int) -> list[dict[str, str]]:
    query = {
        "collection_id": collection_id,
        "max_related_collections": related_num,
        "min_other_collections": 0,
        "max_other_collections": 0,
        "max_total_collections": related_num,
        "max_per_type": 2,  # so that first 3 will not be of the same type
        "name_diversity_ratio": 0.5,
    }

    response = requests.post(url, json=query)

    if not response.ok:
        print(f'WARNING: NameGenerator returned an {response.status_code} for collection {collection_id}')
        print(f'WARNING: NameGenerator response: {response.json()}')
        return []

    related_collections = []
    for related_collection in response.json()['related_collections']:
        related_collections.append({
            'id': related_collection['collection_id'],
            'name': related_collection['title'],
            'types': related_collection['types'],
        })
    return related_collections


def precompute_related_collections(output: str):
    es = connect_to_elasticsearch(CONFIG.elasticsearch)
    collections = collect_existing_collections(es, CONFIG.elasticsearch.index)
    url = 'http://generator:8000/find_collections_by_collection'

    with jsonlines.open(output, 'w') as writer:
        for collection in tqdm.tqdm(collections, desc='precomputing related collections'):
            related_collections = generate_related_collections(url, collection['id'], 10)
            writer.write({
                '_id': collection['id'],
                '_index': CONFIG.elasticsearch.index,
                '_op_type': 'update',
                'doc': {
                    'name_generator': {
                        'related_collections_count': len(related_collections),
                        'related_collections': related_collections,
                    }
                }
            })


def apply_updates(input: str):
    es = connect_to_elasticsearch(CONFIG.elasticsearch)
    with jsonlines.open(input) as reader:
        for ok, result in tqdm.tqdm(
                streaming_bulk(es, reader, index=CONFIG.elasticsearch.index, raise_on_error=False,
                               max_chunk_bytes=1_000_000, max_retries=1),
                total=es.count(index=CONFIG.elasticsearch.index)['count'],
                desc='applying updates'
        ):
            if not ok:
                print(f'WARNING: {result}')


with DAG(
        "precompute-related-collections",
        default_args={
            "email": [CONFIG.email],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "cwd": CONFIG.local_prefix,
            "start_date": CONFIG.start_date,
        },
        description="Runs a local version of NameGenerator and utilizes its endpoint to generate related collections."
                    "Then, it updates the related collections field in the Elasticsearch.",
        schedule=None,  # TODO ??
        start_date=CONFIG.start_date,
        catchup=False,
        tags=["precompute-related-collections", "collection-templates"],
) as dag:

    clone_name_generator_task = BashOperator(
        task_id="clone-name-generator",
        cwd=f"{CONFIG.local_prefix}",
        bash_command=(
            f"rm -rf name-generator && "
            f"git clone -b prod https://{CONFIG.github_token}@github.com/namehash/name-generator.git name-generator && "
            f"cp -f {CONFIG.local_prefix}/docker-compose.yml {CONFIG.local_prefix}/name-generator/docker-compose.yml && "
            f"cp -f {CONFIG.local_prefix}/prod_new.yaml {CONFIG.local_prefix}/name-generator/conf/pipelines/prod_new.yaml"
        ),
        start_date=CONFIG.start_date,
    )
    clone_name_generator_task.doc_md = dedent(
        """\
    #### Clone NameGenerator
    Clones the NameGenerator repository from GitHub. Uses the prod branch.
    """
    )

    launch_name_generator_task = BashOperator(
        task_id="launch-name-generator",
        cwd=f"{CONFIG.local_prefix}/name-generator",
        env={
            'S3_ACCESS_KEY_ID': CONFIG.aws.access_key_id,
            'S3_SECRET_ACCESS_KEY': CONFIG.aws.secret_access_key,
            'ES_HOST': CONFIG.elasticsearch.host,
            'ES_PORT': str(CONFIG.elasticsearch.port),
            'ES_USERNAME': CONFIG.elasticsearch.username,
            'ES_PASSWORD': CONFIG.elasticsearch.password,
            'ES_INDEX': CONFIG.elasticsearch.index,
        },
        bash_command=(
            f"docker compose up -d --build"
        ),
        start_date=CONFIG.start_date,
    )
    launch_name_generator_task.doc_md = dedent(
        """\
    #### Launch NameGenerator
    Launches the NameGenerator service using docker-compose.
    """
    )

    wait_for_name_generator_task = BashOperator(
        task_id="wait-for-name-generator",
        cwd=f"{CONFIG.local_prefix}",
        bash_command=(
            f"./wait-for-it.sh generator:8000 -t 300"
        ),
        start_date=CONFIG.start_date,
    )
    wait_for_name_generator_task.doc_md = dedent(
        """\
    #### Wait for NameGenerator
    Waits for the NameGenerator service to finish loading.
    """
    )

    generate_related_collections_task = PythonOperator(
        task_id="generate-related-collections",
        python_callable=precompute_related_collections,
        op_kwargs={
            'output': PRECOMPUTED_RELATED_COLLECTIONS.local_name(),
        },
        start_date=CONFIG.start_date,
    )
    generate_related_collections_task.doc_md = dedent(
        """\
    #### Generate related collections
    Send requests to NameGenerator to generate related collections.
    """
    )

    stop_name_generator_task = BashOperator(
        task_id="stop-name-generator",
        cwd=f"{CONFIG.local_prefix}/name-generator",
        bash_command=(
            f"docker compose down"
        ),
        start_date=CONFIG.start_date,
    )
    stop_name_generator_task.doc_md = dedent(
        """\
    #### Stop NameGenerator
    Stops the NameGenerator service.
    """
    )

    apply_updates_task = PythonOperator(
        task_id="apply-updates",
        python_callable=apply_updates,
        op_kwargs={
            'input': PRECOMPUTED_RELATED_COLLECTIONS.local_name(),
        },
        start_date=CONFIG.start_date,
    )
    apply_updates_task.doc_md = dedent(
        """\
    #### Apply updates
    Applies the updates to the Elasticsearch.
    """
    )

    clone_name_generator_task >> launch_name_generator_task >> wait_for_name_generator_task \
        >> generate_related_collections_task >> stop_name_generator_task >> apply_updates_task
