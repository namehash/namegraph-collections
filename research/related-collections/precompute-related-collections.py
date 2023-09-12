from argparse import ArgumentParser
import jsonlines
import requests
import tqdm
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


def connect_to_elasticsearch(
        scheme: str,
        host: str,
        port: int,
        username: str,
        password: str,
):
    return Elasticsearch(
        hosts=[{
            'scheme': scheme,
            'host': host,
            'port': port
        }],
        http_auth=(username, password),
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
    res = requests.post(url, json=query)
    related_collections = []
    for related_collection in res.json()['related_collections']:
        related_collections.append({
            'id': related_collection['collection_id'],
            'name': related_collection['title'],
            'types': related_collection['types'],
        })
    return related_collections


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--endpoint', type=str, required=True, help='URL to the NameGenerator endpoint')
    parser.add_argument('--output', type=str, required=True, help='Path to the output JSONL file')
    parser.add_argument('--related_num', type=int, default=10, help='Number of related collections to generate')
    args = parser.parse_args()

    host = os.getenv('ES_HOST', 'localhost')
    port = int(os.getenv('ES_PORT', '9200'))
    username = os.getenv('ES_USERNAME', 'elastic')
    password = os.getenv('ES_PASSWORD', 'espass')
    index = os.getenv('ES_INDEX', 'collection-templates-1')

    es = connect_to_elasticsearch(
        scheme='http' if host in ['localhost', '127.0.0.1'] else 'https',
        host=host, port=port, username=username, password=password,
    )

    existing_collections = collect_existing_collections(es, index)
    with jsonlines.open(args.output, 'w') as writer:
        for collection in tqdm.tqdm(existing_collections, desc='precomputing related collections'):
            related_collections = generate_related_collections(args.endpoint, collection['id'], args.related_num)
            collection['related_collections'] = related_collections
            writer.write(collection)
