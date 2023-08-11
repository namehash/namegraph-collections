from argparse import ArgumentParser
from typing import Any, Optional
import jsonlines
import hashlib
import json
import tqdm
import time
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


def collect_ids_mapping(es: Elasticsearch, index: str) -> dict[str, str]:
    query = {
        'query': {
            'match_all': {}
        },
        '_source': ['metadata.id'],
    }

    mapping = dict()
    for doc in tqdm.tqdm(
            scan(es, index=index, query=query),
            total=es.count(index=index)['count'],
            desc='collecting IDs mapping'
    ):
        mapping[doc['_source']['metadata']['id']] = doc['_id']

    return mapping


class JSONLIndex:
    def __init__(self, filepath: str, fields: list[str], ids_mapping: dict[str, str]):
        self.filepath = filepath
        self.fields = sorted(fields)
        self.ids_mapping = ids_mapping

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
            value = get_nested_field(document, field)
            hash.update(json.dumps(value, sort_keys=True).encode('utf-8'))
        return hash.hexdigest()

    def get_hash(self, id: str) -> str:
        return self.id2hash[id]

    def get_document(self, id: str) -> tuple[str, dict]:
        with open(self.filepath, 'r', encoding='utf-8') as f:
            f.seek(self.id2offset[id])
            document = json.loads(f.readline())
            return self.ids_mapping[document['metadata']['id']], document


def get_nested_field(document: dict, field: str, default: Any = None) -> Any:
    value = document
    for part in field.split('.'):
        value = value.get(part, None)
        if value is None:
            return default
    return value


def set_nested_field(document: dict, field: str, value: Any) -> None:
    placeholder = document
    splitted_field = field.split('.')
    for part in splitted_field[:-1]:
        placeholder = placeholder.setdefault(part, {})
    placeholder[splitted_field[-1]] = value


def prepare_insert(collection: dict, index: str) -> Optional[dict[str, Any]]:
    if collection['metadata']['members_count'] > 10000:
        # print('Skipping collection with more than 10000 members')
        return None

    collection['template']['nonavailable_members_count'] += 1
    collection['template']['invalid_members_count'] += 1

    return {
        '_op_type': 'index',
        '_index': index,
        '_source': collection
    }


def prepare_update(
        es_id: str,
        old_collection: dict,
        collection: dict,
        fields: list[str],
        index: str
) -> Optional[dict[str, Any]]:

    if collection['metadata']['members_count'] > 10000:
        # print('Skipping collection with more than 10000 members')
        return None

    # build the body of the update request
    doc = dict()

    # check if the fields have changed (the fields are separated by dots)
    for field in fields:
        old_value = get_nested_field(old_collection, field)
        new_value = get_nested_field(collection, field)

        # if both values are dicts, we can update only the changed fields
        if isinstance(old_value, dict) and isinstance(new_value, dict):
            old_keys = set(old_value.keys())
            new_keys = set(new_value.keys())

            # if some keys are missing, update the whole dict
            if old_keys - new_keys:
                set_nested_field(doc, field, new_value)
            else:
                # check if the new values are different or not present at all
                for key in new_keys:
                    if key not in old_keys or old_value[key] != new_value[key]:
                        set_nested_field(doc, field + '.' + key, new_value[key])

        # if the values are totally different, update the whole field
        elif old_value != new_value:
            set_nested_field(doc, field, new_value)

    # this update happens only in populate.py, so it is not presented in the previous JSONL file
    if 'template' in doc:
        if 'nonavailable_members_count' in doc['template']:
            doc['template']['nonavailable_members_count'] += 1
        if 'invalid_members_count' in doc['template']:
            doc['template']['invalid_members_count'] += 1

    # if no fields have changed, do nothing
    if not doc:
        return None

    return {
        '_op_type': 'update',
        '_index': index,
        '_id': es_id,
        'doc': doc
    }


if __name__ == '__main__':
    parser = ArgumentParser(description="Analyze old and new JSONL with collections, calculate needed updates and execute or write to JSONL.")
    parser.add_argument('input', help='input JSONL file with current collections')
    parser.add_argument('previous', help='input JSONL file with previous collections')
    parser.add_argument('--output', default=None, help='output JSONL file with updates or inserts')
    parser.add_argument('--comparing_fields', nargs='+',
                        default=['data', 'template', 'metadata.members_count',
                                 'metadata.collection_name_log_probability'],
                        help='fields for hash calculating')
    parser.add_argument('--updating_fields', nargs='+',
                        default=['data', 'template', 'metadata.modified',
                                 'metadata.members_count', 'metadata.collection_name_log_probability'],
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
    ids_mapping = collect_ids_mapping(es, index)
    covered_ids = set()

    jsonl_index = JSONLIndex(args.previous, args.comparing_fields, ids_mapping)

    writer = jsonlines.open(args.output, 'w') if args.output else None

    print('Building index...')
    t0 = time.perf_counter()
    jsonl_index.build()
    print(f'Index built in {time.perf_counter() - t0:.2f} seconds.')

    print('Updating Elasticsearch...')
    t0 = time.perf_counter()

    with jsonlines.open(args.input, 'r') as reader:
        for collection in tqdm.tqdm(reader):
            collection_id = collection['metadata']['id']
            covered_ids.add(collection_id)

            # FIXME remove this
            del collection['data']['avatar_emoji']
            del collection['data']['avatar_image']
            del collection['data']['banner_image']

            # we insert only if the collection is not in the Elasticsearch index
            # disregarding the fact that it might be in the previous JSONL file (insertion might have failed)
            # it also prevents from inserting duplicates (but mapping should be updated each time we run this script)
            if collection_id not in jsonl_index.ids_mapping:
                # print('New collection:', collection_id)
                # TODO to optimize further, we can aggregate a part of the new collections, and bulk insert them
                operation = prepare_insert(collection, index)
                if operation is not None:
                    if writer:
                        writer.write(operation)
                    else:
                        es.index(index=index, body=operation['_source'])
                continue

            # it has probably been inserted already, since it is in the Elasticsearch, but not in the previous JSONL
            if collection_id not in jsonl_index.id2hash:
                continue

            previous_hash = jsonl_index.get_hash(collection_id)
            current_hash = jsonl_index.hash(collection)

            if previous_hash == current_hash:
                continue

            es_id, old_collection = jsonl_index.get_document(collection_id)

            # print('Updated collection:', collection_id)
            operation = prepare_update(es_id, old_collection, collection, args.updating_fields, index)
            if operation is not None:
                if writer:
                    writer.write(operation)
                else:
                    es.update(index=index, id=es_id, body={'doc': operation['doc']})

    print(f'Elasticsearch updated in {time.perf_counter() - t0:.2f} seconds.')

    print('Setting archived flag for collections that are not in the input file...')
    t0 = time.perf_counter()
    for collection_id, es_id in tqdm.tqdm(ids_mapping.items(), total=len(ids_mapping)):
        if collection_id not in covered_ids:
            doc = {'data': {'archived': True}}
            if writer:
                writer.write({'_op_type': 'update', '_index': index, '_id': es_id, 'doc': doc})
            else:
                es.update(index=index, id=es_id, body={'doc': doc})

    print(f'Archived flag set in {time.perf_counter() - t0:.2f} seconds.')

    if writer:
        writer.close()
