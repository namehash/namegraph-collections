from argparse import ArgumentParser
import jsonlines
import tqdm
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


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


if __name__ == '__main__':
    parser = ArgumentParser(description="Execute ES update operations from JSONL")
    parser.add_argument('input', help='input JSONL file with operations to execute')
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

    with jsonlines.open(args.input, 'r') as reader:
        # for action in tqdm.tqdm(reader):
        #     if action['_op_type'] == 'update':
        #         es.update(index=index, id=action['_id'], body={'doc': action['doc']})
        #     elif action['_op_type'] == 'index':
        #         es.index(index=index, body=action['_source'])
        #     else:
        #         raise ValueError(f'Unknown action type: {action["_op_type"]}')

        progress = tqdm.tqdm(unit="actions")
        successes = 0

        for ok, action in streaming_bulk(client=es,
                                         index=index,
                                         actions=reader,
                                         max_chunk_bytes=1_000_000,  # 1mb
                                         raise_on_error=False,
                                         raise_on_exception=False,
                                         max_retries=1):
            progress.update(1)
            successes += ok

            if not ok:
                print(action)
                # if action['_op_type'] == 'insert':
                #     print('Failed insert:', action['_source']['metadata']['id'])
                # elif action['_op_type'] == 'update':
                #     print('Failed update:', action['_id'])
                # else:
                #     print('Failed action:', action['_op_type'])
