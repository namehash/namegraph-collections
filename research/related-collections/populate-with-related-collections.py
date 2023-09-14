from argparse import ArgumentParser
from typing import Iterable
import jsonlines
import tqdm
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk


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


def collect_members_count(es: Elasticsearch, index: str) -> dict[str, int]:
    query = {
        'query': {
            'match_all': {}
        },
        '_source': ['metadata.members_count'],
    }

    collections = dict()
    for doc in tqdm.tqdm(
            scan(es, index=index, query=query),
            total=es.count(index=index)['count'],
            desc='collecting members count'
    ):
        collections[doc['_id']] = doc['_source']['metadata']['members_count']

    return collections


def populate_with_related_collections(es: Elasticsearch, index: str, generator: Iterable[dict[str, str]]):
    for ok, result in tqdm.tqdm(
            streaming_bulk(es, generator, index=index, max_chunk_bytes=1_000_000, max_retries=1),
            total=es.count(index=index)['count'],
            desc='populating with related collections'
    ):
        if not ok:
            print(result)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='JSONL file with the precomputed related collections')
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

    collection_id2members_count = collect_members_count(es, index)

    def ops_generator():
        with jsonlines.open(args.input) as reader:
            for doc in reader:
                related_collections = []
                for related_collection in doc['related_collections']:
                    related_collection_id = related_collection['id']
                    related_collections.append({
                        'collection_id': related_collection_id,
                        'collection_name': related_collection['name'],
                        'members_count': collection_id2members_count[related_collection_id],
                    })

                yield {
                    '_op_type': 'update',
                    '_index': index,
                    '_id': doc['id'],
                    'doc': {
                        'name_generator': {
                            'related_collections_count': len(related_collections),
                            'related_collections': related_collections,
                        }
                    }
                }

    populate_with_related_collections(es, index, ops_generator())
