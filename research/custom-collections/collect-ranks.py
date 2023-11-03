from argparse import ArgumentParser
import tqdm
import json
import os

import matplotlib.pyplot as plt
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


def collect_ranks(es: Elasticsearch, index: str) -> tuple[dict[str, list[int]], dict[str, dict[str, int]]]:
    query = {
        'query': {
            'match_all': {}
        },
        '_source': ['data.collection_name', 'template.collection_rank',
                    'template.names.normalized_name', 'template.names.rank']
    }
    res = scan(es, index=index, query=query, size=1000)
    collection_ranks = dict()
    members_ranks = dict()
    for hit in tqdm.tqdm(res, total=es.count(index=index)['count']):
        source = hit['_source']
        collection_name = source['data']['collection_name']
        collection_ranks[collection_name] = source['template']['collection_rank']
        members_ranks[collection_name] = {
            name['normalized_name']: name['rank']
            for name in source['template']['names']
        }
    return collection_ranks, members_ranks


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--output', type=str, required=True, help='Path to the output JSON file')
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

    collection_ranks, members_ranks = collect_ranks(es, index)
    with open(args.output, 'w') as f:
        json.dump({
            'collection_ranks': collection_ranks,
            'members_ranks': members_ranks
        }, f)
