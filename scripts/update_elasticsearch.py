from argparse import ArgumentParser
import jsonlines
import hashlib
import json
import tqdm
import time
import os

from elasticsearch import Elasticsearch


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


class JSONLIndex:
    def __init__(self, filepath: str, fields: list[str]):
        self.filepath = filepath
        self.fields = sorted(fields)

        self.id2hash = dict()
        self.id2offset = dict()

    def build(self) -> None:
        with open(self.filepath, 'r', encoding='utf-8') as f:
            while True:
                start_pos = f.tell()
                line = f.readline()

                # EOF, empty lines before are returned as '\n'
                if not line:
                    break

                # skip empty lines
                if line == '\n':
                    continue

                # skip invalid JSON lines
                try:
                    data = json.loads(line)
                    obj_id = data['metadata']['id']
                except json.JSONDecodeError:
                    print('JSONDecodeError:', line)
                    continue

                self.id2hash[obj_id] = self.hash(data)
                self.id2offset[obj_id] = start_pos

    def hash(self, document: dict) -> str:
        hash = hashlib.sha256()
        for field in self.fields:
            # field may be a path to a nested field
            value = document
            for part in field.split('.'):
                value = value.get(part, None)
                if value is None:
                    break

            hash.update(json.dumps(value, sort_keys=True).encode('utf-8'))
        return hash.hexdigest()
        # return hashlib.sha256(json.dumps(document, sort_keys=True).encode('utf-8')).hexdigest()

    def get_hash(self, id: str) -> str:
        return self.id2hash[id]

    def get_document(self, id: str) -> dict:
        with open(self.filepath, 'r', encoding='utf-8') as f:
            f.seek(self.id2offset[id])
            return json.loads(f.readline())


def insert(es: Elasticsearch, collection: dict) -> None:
    # FIXME

    es.index(
        index=index,
        id=collection['metadata']['id'],
        body=collection,
    )


def update(es: Elasticsearch, old_collection: dict, collection: dict, fields: list[str]) -> None:
    # bulk update the fields specified in the fields list, if they have changed
    # otherwise, do nothing

    # build the body of the update request
    body = {
        'doc': {}
    }

    # check if the fields have changed (the fields are separated by dots)
    for field in fields:
        # get the value of the field in the old collection
        old_value = old_collection
        for part in field.split('.'):
            old_value = old_value.get(part, None)
            if old_value is None:
                break

        # get the value of the field in the new collection
        new_value = collection
        for part in field.split('.'):
            new_value = new_value.get(part, None)
            if new_value is None:
                break

        # if the values are different, update the field
        # FIXME: this will not work for nested fields
        if old_value != new_value:
            body['doc'][field] = new_value

    # if no fields have changed, do nothing
    if not body['doc']:
        return

    es.update(
        index=index,
        id=collection['metadata']['id'],
        body=body
    )


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='input JSONL file with current collections')
    parser.add_argument('previous', help='input JSONL file with previous collections')
    parser.add_argument('--fields', nargs='+',
                        default=['metadata.title', 'metadata.description', 'metadata.keywords'],
                        help='fields to compare')
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

    jsonl_index = JSONLIndex(args.previous, args.fields)

    print('Building index...')
    t0 = time.perf_counter()
    jsonl_index.build()
    print(f'Index built in {time.perf_counter() - t0:.2f} seconds.')

    print('Updating Elasticsearch...')
    t0 = time.perf_counter()
    with jsonlines.open(args.input, 'r') as reader:
        for collection in tqdm.tqdm(reader):
            collection_id = collection['metadata']['id']
            if collection_id not in jsonl_index.id2hash:
                print('New collection:', collection_id)
                # TODO to optimize further, we can aggregate a part of the new collections, and bulk insert them
                # insert(es, collection)
                continue

            previous_hash = jsonl_index.get_hash(collection_id)
            current_hash = jsonl_index.hash(collection)

            if previous_hash == current_hash:
                continue

            old_collection = jsonl_index.get_document(collection_id)

            print('Updated collection:', collection_id)
            # update(es, old_collection, collection, args.fields)

    print(f'Elasticsearch updated in {time.perf_counter() - t0:.2f} seconds.')
