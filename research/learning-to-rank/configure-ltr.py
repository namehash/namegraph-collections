from argparse import ArgumentParser
from pprint import pprint
import json
import os

from elasticsearch import Elasticsearch
from elasticsearch_ltr import LTRClient


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


def extract_names_from_hits(hits: dict) -> list[str]:
    return [hit['_source']['name'] for hit in hits['hits']['hits']]


def rank_feature(feature_field, feature_name=None):
    if feature_name is None:
        feature_name = feature_field[feature_field.rindex(".")+1:]
    return {
            "name": feature_name,
            "params": [],
            "template": {
                "rank_feature": {
                    "field": feature_field,
                }
            }
        }


def keyword_feature(feature_field, feature_name=None):
    if feature_name is None:
        feature_name = feature_field[feature_field.rindex(".")+1:]
    return {
        "name": feature_name,
        "params": ["keywords"],
        "template": {
            "match": {
                feature_field: "{{keywords}}"
            }
        }
    }


def log_feature(feature_field, feature_name=None):
    if feature_name is None:
        feature_name = feature_field[feature_field.rindex(".")+1:]
    return {
            "name": f"{feature_name}_log",
            "params": [],
            "template": {
                "rank_feature": {
                    "field": feature_field,
                    "log": {"scaling_factor": 1}
                }
            }
        }


def size_feature(feature_field, feature_name=None):
    if feature_name is None:
        feature_name = feature_field[feature_field.rindex(".")+1:]

    return {
            "name": feature_name,
            "params": [],
            "template_language": "script_feature",
            "template": {
                "lang": "painless",
                "source": f"params['_source'].{feature_field}.size()",
                "params": {}
            }
    }


def feature_set_definition() -> dict:
    return {
        "featureset": {
            "features": [
                keyword_feature("data.collection_name"),
                keyword_feature("data.collection_name.exact"),
                keyword_feature("data.collection_description"),
                keyword_feature("data.collection_keywords"),
                keyword_feature("data.names.normalized_name"),
                keyword_feature("data.names.tokenized_name"),
                rank_feature("template.collection_rank"),
                rank_feature("template.members_rank_mean"),
                rank_feature("template.members_rank_median"),
                rank_feature("template.members_system_interesting_score_mean"),
                rank_feature("template.members_system_interesting_score_median"),
                rank_feature("template.valid_members_count"),
                rank_feature("template.invalid_members_count"),
                rank_feature("template.valid_members_ratio"),
                rank_feature("template.nonavailable_members_count"),
                rank_feature("template.nonavailable_members_ratio"),
                log_feature("template.collection_rank"),
                log_feature("template.members_rank_mean"),
                log_feature("template.members_rank_median"),
                log_feature("template.valid_members_count"),
                log_feature("template.nonavailable_members_count"),
                size_feature("data.names")
                #rank_feature("template.is_merged"),
            ]
        }
    }


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--metadata_index', type=str, help='metadata index name')
    parser.add_argument('--feature_set', type=str, help='feature set name')
    parser.add_argument('--model_name', type=str, help='model name')
    parser.add_argument('--model_path', type=str, help='path to model definition')
    parser.add_argument('--reset', action='store_true', help='reset the previous configuration')
    args = parser.parse_args()

    host = os.getenv('ES_HOST', 'localhost')
    port = int(os.getenv('ES_PORT', '9200'))
    username = os.getenv('ES_USERNAME', 'elastic')
    password = os.getenv('ES_PASSWORD', 'espass')

    es = connect_to_elasticsearch(
        scheme='http' if host in ['localhost', '127.0.0.1'] else 'https',
        host=host, port=port, username=username, password=password,
    )
    ltr = LTRClient(es)

    # Create a feature store (metadata index)
    features_stores = ltr.list_feature_stores()['stores']
    print('Existing feature stores')
    pprint(features_stores)
    if args.metadata_index in features_stores:
        if args.reset:
            ltr.delete_feature_store(name=args.metadata_index)
        else:
            raise ValueError(f'Feature store {args.metadata_index} already exists')

    ltr.create_feature_store(name=args.metadata_index)
    print('After creating feature store')
    pprint(ltr.list_feature_stores())
    print()

    # Create a feature set
    feature_sets = extract_names_from_hits(ltr.list_feature_sets(store_name=args.metadata_index))
    print('Existing feature sets')
    pprint(feature_sets)
    if args.feature_set in feature_sets:
        if args.reset:
            ltr.delete_feature_set(name=args.feature_set, store_name=args.metadata_index)
        else:
            raise ValueError(f'Feature set {args.feature_set} already exists')

    ltr.create_feature_set(
        name=args.feature_set,
        body=feature_set_definition(),
        store_name=args.metadata_index,
        headers={'Content-Type': 'application/json'},
    )
    print('After creating feature set')
    pprint(extract_names_from_hits(ltr.list_feature_sets(store_name=args.metadata_index)))
    print()

    # Create a model
    models = extract_names_from_hits(ltr.list_models(store_name=args.metadata_index))
    print('Existing models')
    pprint(models)
    if args.model_name in models:
        if args.reset:
            ltr.delete_model(name=args.model_name, store_name=args.metadata_index)
        else:
            raise ValueError(f'Model {args.model_name} already exists')

    with open(args.model_path, 'r') as f:
        model_definition = f.read()

    ltr.create_model(
        name=args.model_name,
        body={
            "model": {
                "name": args.model_name,
                "model": {
                    "type": "model/ranklib",
                    "definition": model_definition
                }
            }
        },
        store_name=args.metadata_index,
        feature_set_name=args.feature_set,
        headers={'Content-Type': 'application/json'},
    )
    print('After creating model')
    pprint(extract_names_from_hits(ltr.list_models(store_name=args.metadata_index)))
