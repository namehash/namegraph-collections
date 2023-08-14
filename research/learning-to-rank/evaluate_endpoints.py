from argparse import ArgumentParser
from copy import deepcopy
import base64
import json
import tqdm
import time
import math
import io
import os

import numpy as np
import matplotlib.pyplot as plt
from elasticsearch import Elasticsearch

from argparse import ArgumentParser
from typing import Optional
import requests
import time
import tqdm

import numpy as np


def request_generator_grouped_http(
        query: str,
        name_diversity_ratio: Optional[float] = 0.5,
        max_per_type: Optional[int] = 2,
        enable_learning_to_rank: Optional[bool] = True,
        host: str = 'localhost',
        port: int = 8000,
):
    data = {
        "label": query,
        "metadata": False,
        "min_primary_fraction": 0.1,
        "params": {
            "country": "us",
            "mode": "full",
            "enable_learning_to_rank": enable_learning_to_rank,
            "name_diversity_ratio": name_diversity_ratio,
            "max_per_type": max_per_type
        }
    }
    result = requests.post(
        f'http://{host}:{port}/grouped_by_category',
        json=data,
    ).json()

    collections = []
    for category in result['categories']:
        if category['type'] == 'related':
            collections.append(Collection(title=category['collection_title'], types=[], avatar_emoji='',
                                          number_of_names=category['collection_members_count'],
                                          names=[n['name'] for n in category['suggestions']]))

    return collections, 1, 1


def request_generator_http(
        query: str,
        limit: int,
        name_diversity_ratio: Optional[float],
        max_per_type: Optional[int],
        limit_names: Optional[int],
        sort_order: Optional[str],
        host: str = 'localhost',
        port: int = 8000,
):
    data = {
        "query": query,
        "max_related_collections": limit,
        "min_other_collections": 0,
        "max_other_collections": 0,
        "max_total_collections": limit,
        "name_diversity_ratio": name_diversity_ratio,
        "max_per_type": max_per_type,
        "limit_names": limit_names,
        "sort_order": sort_order,
    }
    return requests.post(
        f'http://{host}:{port}/find_collections_by_string',
        json=data,
    ).json()


def search_by_all(
        query: str,
        limit: int,
        name_diversity_ratio: Optional[float],
        max_per_type: Optional[int],
        limit_names: Optional[int],
        sort_order: Optional[str],
        host: str = 'localhost',
        port: int = 8000,
):
    response = request_generator_http(
        query,
        limit,
        name_diversity_ratio,
        max_per_type,
        limit_names,
        sort_order,
        host=host,
        port=port,
    )
    return response['related_collections'], \
        response['metadata']['processing_time_ms'], \
        response['metadata']['elasticsearch_processing_time_ms']


class Collection:
    def __init__(self, title, types, avatar_emoji, number_of_names, names):
        self.title = title
        self.types = types
        self.avatar_emoji = avatar_emoji
        self.number_of_names = number_of_names
        self.names = names


from joblib import Memory


# memory = Memory()


def search_with_latency(
        query: str,
        limit: int = 10,
        name_diversity_ratio: Optional[float] = 0.5,
        max_per_type: Optional[int] = 3,
        limit_names: Optional[int] = 10,
        sort_order: Optional[str] = 'ES',
        host: str = 'localhost',
        port: int = 8000,
):
    results, latency, took = search_by_all(
        query,
        limit,
        name_diversity_ratio,
        max_per_type,
        limit_names,
        sort_order,
        host=host,
        port=port
    )
    # print(results)
    collections = [
        Collection(r['title'], r['types'], r['avatar_emoji'], r['number_of_names'], [n['name'] for n in r['top_names']])
        for r in
        results]
    titles = [hit['title'] for hit in results]
    return collections, latency, took


LTR_QUERY = {
    "query": {
        "bool": {
            "filter": [{"term": {"data.public": True}}],
            "must": [
                {
                    "multi_match": {
                        "fields": [
                            "data.collection_name^3",  # 1
                            "data.collection_name.exact^3",  # 2
                            "data.collection_description",  # 3
                            "data.collection_keywords^2",  # 4
                            "data.names.normalized_name",  # 5
                            "data.names.tokenized_name",  # 6
                        ],
                        "type": "most_fields",
                    }
                }
            ],
            "should": [
                {
                    "rank_feature": {"field": "template.collection_rank",  # 7
                                     "boost": 100, }
                },
                {
                    "rank_feature": {"field": "metadata.members_rank_mean",  # 8
                                     "boost": 1, }
                },
                {
                    "rank_feature": {"field": "template.members_rank_median",  # 9
                                     "boost": 1, }
                },
                {
                    "rank_feature": {"field": "template.members_system_interesting_score_mean",  # 10
                                     "boost": 1, }
                },
                {
                    "rank_feature": {"field": "template.members_system_interesting_score_median",  # 11
                                     "boost": 1, }
                },
                {
                    "rank_feature": {"field": "template.valid_members_count",  # 12
                                     "boost": 1, }
                },
                {
                    "rank_feature": {"field": "template.invalid_members_count",  # 13
                                     "boost": 1, }
                },
                {
                    "rank_feature": {"field": "template.valid_members_ratio",  # 14
                                     "boost": 1, }
                },
                {
                    "rank_feature": {"field": "template.nonavailable_members_count",  # 15
                                     "boost": 1, }
                },
                {
                    "rank_feature": {"field": "template.nonavailable_members_ratio",  # 16
                                     "boost": 1, }
                },
            ]
        }

    },
    '_source': False,
    'fields': ['metadata.id',
               'data.collection_name',
               'template.collection_rank',
               'metadata.owner',
               'metadata.members_count',
               'template.top10_names.normalized_name',
               'template.top10_names.namehash',
               'data.avatar_emoji',
               'template.collection_types',
               'metadata.modified'],
    "size": 10,
    "rescore": {
        "window_size": 50,
        "query": {
            "rescore_query": {
                "sltr": {
                    "_name": "logged_featureset",
                    "store": "ltr-metadata-index",
                    "featureset": "ltr-feature-set",
                    "model": "exp6-8",
                    "params": {
                        # "keywords": "rambo"
                    },
                }
            },
            "query_weight": 0,

        }
    }
}

LTR_QUERY2 = {
    "query": {
        "bool": {
            "filter": [{"term": {"data.public": True}}],
            "must": [
                {
                    "multi_match": {
                        "fields": [
                            "data.collection_name^3",  # 1
                            "data.collection_name.exact^3",  # 2
                            "data.collection_keywords^2",  # 4
                            "data.names.normalized_name",  # 5
                            "data.names.tokenized_name",  # 6
                        ],
                        "type": "cross_fields",
                    }
                }
            ],
            "should": [
                {"rank_feature": {"field": "template.collection_rank", "boost": 100}},
                {"rank_feature": {"field": "metadata.members_count"}},
                {"rank_feature": {"field": "template.members_rank_mean"}},
                {"rank_feature": {"field": "template.members_system_interesting_score_median"}},
                {"rank_feature": {"field": "template.valid_members_ratio"}},
                {"rank_feature": {"field": "template.nonavailable_members_ratio", "boost": 100}}
            ]
        }

    },
    '_source': False,
    'fields': ['metadata.id',
               'data.collection_name',
               'template.collection_rank',
               'metadata.owner',
               'metadata.members_count',
               'template.top10_names.normalized_name',
               'template.top10_names.namehash',
               'data.avatar_emoji',
               'template.collection_types',
               'metadata.modified'],
    "size": 10,
    "rescore": {
        "window_size": 50,
        "query": {
            "rescore_query": {
                "sltr": {
                    "_name": "logged_featureset",
                    "store": "ltr-metadata-index",
                    "featureset": "ltr-feature-set",
                    "model": "exp6-8",
                    "params": {
                        # "keywords": "rambo"
                    },
                }
            },
            "query_weight": 0,

        }
    }
}

LTR_QUERY2b = deepcopy(LTR_QUERY2)
LTR_QUERY2b['query']['bool']['must'][0]['multi_match']['type'] = 'most_fields'

NO_LTR_QUERY = {
    "query": {
        "bool": {
            "filter": [{"term": {"data.public": True}}],
            "must": [
                {
                    "multi_match": {
                        "fields": [
                            "data.collection_name^3",
                            "data.collection_name.exact^3",
                            "data.collection_keywords^2",
                            "data.names.normalized_name",
                            "data.names.tokenized_name",
                        ],
                        "type": "cross_fields",
                    }
                }
            ],
            "should": [
                {"rank_feature": {"field": "template.collection_rank", "boost": 100}},
                {"rank_feature": {"field": "metadata.members_count"}},
            ]
        }

    },
    '_source': False,
    'fields': ['metadata.id',
               'data.collection_name',
               'template.collection_rank',
               'metadata.owner',
               'metadata.members_count',
               'template.top10_names.normalized_name',
               'template.top10_names.namehash',
               'template.collection_types',
               'data.avatar_emoji',
               'metadata.modified'],
    "size": 10,
}

NO_LTR_QUERY2 = {
    "query": {
        "bool": {
            "filter": [{"term": {"data.public": True}}],
            "must": [
                {
                    "multi_match": {
                        "fields": [
                            "data.collection_name^3",
                            "data.collection_name.exact^3",
                            "data.collection_keywords^2",
                            "data.names.normalized_name",
                            "data.names.tokenized_name",
                        ],
                        "type": "cross_fields",
                    }
                }
            ],
            "should": [
                {"rank_feature": {"field": "template.collection_rank", "boost": 100}},
                {"rank_feature": {"field": "metadata.members_count"}},
                {"rank_feature": {"field": "template.members_rank_mean"}},
                {"rank_feature": {"field": "template.members_system_interesting_score_median"}},
                {"rank_feature": {"field": "template.valid_members_ratio"}},
                {"rank_feature": {"field": "template.nonavailable_members_ratio", "boost": 100}}
            ]
        }

    },
    '_source': False,
    'fields': ['metadata.id',
               'data.collection_name',
               'template.collection_rank',
               'metadata.owner',
               'metadata.members_count',
               'template.top10_names.normalized_name',
               'template.top10_names.namehash',
               'template.collection_types',
               'data.avatar_emoji',
               'metadata.modified'],
    "size": 10,
}
NO_LTR_QUERY2b = deepcopy(NO_LTR_QUERY2)
NO_LTR_QUERY2b['query']['bool']['must'][0]['multi_match']['type'] = 'most_fields'
NO_LTR_QUERY2c = deepcopy(NO_LTR_QUERY2)
NO_LTR_QUERY2c['query']['bool']['must'][0]['multi_match']['type'] = 'best_fields'


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


# @memory.cache(ignore=['es'])
def search(es: Elasticsearch, index: str, query: str, es_query, window_size: int = 1, size: int = 10,
           mode: str = 'ltr') -> tuple[dict, float, float]:
    query_body = deepcopy(es_query)
    if mode == 'ltr':
        query_body['rescore']['query']['rescore_query']['sltr']['params']['keywords'] = query
        query_body['rescore']['window_size'] = window_size

    query_body['query']['bool']['must'][0]['multi_match']['query'] = query

    t0 = time.perf_counter()
    # print(query_body)
    result = es.search(
        index=index,
        body=query_body,
        # size=size,
    )
    t1 = time.perf_counter()
    # print(result)

    collections = [Collection(hit['fields']['data.collection_name'][0],
                              hit['fields']['template.collection_types'][1::2],
                              hit['fields']['data.avatar_emoji'][0],
                              int(hit['fields']['metadata.members_count'][0]),
                              hit['fields']['template.top10_names.normalized_name']) for hit in result['hits']['hits']]

    return collections, (t1 - t0) * 1000, result['took']


def ndcg(scores: list[float], annotated_scores: list[float], k: int, default_score: float = 2.0) -> float:
    scores = scores[:k]
    ideal_scores = sorted(annotated_scores, reverse=True)[:k]

    print('  scores', scores)
    print('  ideal_scores', ideal_scores)

    dcg = 0
    for i, score in enumerate(scores):
        dcg += score / math.log2(i + 2)

    idcg = 0
    for i, score in enumerate(ideal_scores):
        idcg += max(score, default_score) / math.log2(i + 2)

    for j in range(i + 1, k):
        idcg += default_score / math.log2(j + 2)

    ndcg = dcg / idcg if idcg > 0 else 0.0

    print('  ', ndcg)
    print()

    return ndcg


def fig_to_base64(fig):
    img = io.BytesIO()
    fig.savefig(img, format='png',
                bbox_inches='tight')
    img.seek(0)

    return base64.b64encode(img.getvalue())


def binarize_score(scores):
    return [1 if score >= 4 else 0 for score in scores]


if __name__ == '__main__':
    parser = ArgumentParser(description="Evaluate endpoints (i.e. ES, API) and generate reports")
    parser.add_argument('scores', help='file with human annotated scores')
    parser.add_argument('-o', '--output', default='window_sizes.html', help="file with the output report")
    # parser.add_argument('--limit', default=10, type=int, help='limit the number of collections to retrieve')
    parser.add_argument('--repeats', default=1, type=int, help='number of times to repeat the query')
    parser.add_argument('--cachedir', default=None, help='cache directory for calls to ESa nd API')
    # parser.add_argument('--window_sizes', nargs='+', type=int, help='window sizes to test',
    #                     default=[1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 350, 500, 750, 1000])
    args = parser.parse_args()

    memory = Memory(args.cachedir)

    host = os.getenv('ES_HOST', 'localhost')
    port = int(os.getenv('ES_PORT', '9200'))
    username = os.getenv('ES_USERNAME', 'elastic')
    password = os.getenv('ES_PASSWORD', 'espass')
    index = os.getenv('ES_INDEX', 'collection-templates-1')

    nghost = os.getenv('NG_HOST', 'localhost')
    ngport = int(os.getenv('NG_PORT', '8000'))

    # print(request_generator_grouped_http('crypto', 0.5, 2, True, host=nghost, port=ngport))
    # import sys
    # 
    # sys.exit()

    es = connect_to_elasticsearch(
        scheme='http' if host in ['localhost', '127.0.0.1'] else 'https',
        host=host, port=port, username=username, password=password,
    )

    human_scores = dict()

    with open(args.scores) as input:
        for idx, row in enumerate(input):
            data = json.loads(row.strip())
            query = list(data.keys())[0]
            human_scores[query] = {}
            for result, score in data[query].items():
                human_scores[query][result] = score

    queries = list(human_scores.keys())

    # different endpoints with different params, maybe also allow direct ES?
    cached_search = memory.cache(search, ignore=['es'], verbose=False)
    cached_search_with_latency = memory.cache(search_with_latency, verbose=False)
    cached_request_generator_grouped_http = memory.cache(request_generator_grouped_http, verbose=False)

    endpoints = {
        "API_grouped_ltr_n0.5_t2": lambda query: cached_request_generator_grouped_http(query, host=nghost, port=ngport),
        "API_grouped_ltr_nNone_tNone": lambda query: cached_request_generator_grouped_http(query, host=nghost, port=ngport, name_diversity_ratio=None, max_per_type=None),
        "API_grouped_no-ltr_nNone_tNone": lambda query: cached_request_generator_grouped_http(query, host=nghost, port=ngport, name_diversity_ratio=None, max_per_type=None, enable_learning_to_rank=False),
        "API_grouped_no-ltr_n0.5_t2": lambda query: cached_request_generator_grouped_http(query, host=nghost, port=ngport, enable_learning_to_rank=False),
        "API_no-ltr_n0.5_t3": lambda query: cached_search_with_latency(query, host=nghost, port=ngport),
        "API_no-ltr_nNone_tNone": lambda query: cached_search_with_latency(query, host=nghost, port=ngport,
                                                                           name_diversity_ratio=None,
                                                                           max_per_type=None),
        "ES_no-ltr_query2": lambda query: cached_search(es, index, query, es_query=NO_LTR_QUERY2, mode='noltr'),
        "ES_no-ltr_query2most": lambda query: cached_search(es, index, query, es_query=NO_LTR_QUERY2b, mode='noltr'),
        "ES_no-ltr_query2best": lambda query: cached_search(es, index, query, es_query=NO_LTR_QUERY2c, mode='noltr'),
        "ES_no-ltr": lambda query: cached_search(es, index, query, es_query=NO_LTR_QUERY, mode='noltr'),

        # TODO collections from grouped
        "ES_ltr_ws1": lambda query: cached_search(es, index, query, es_query=LTR_QUERY, window_size=1, mode='ltr'),
        "ES_ltr_ws20": lambda query: cached_search(es, index, query, es_query=LTR_QUERY, window_size=20, mode='ltr'),
        "ES_ltr_ws100": lambda query: cached_search(es, index, query, es_query=LTR_QUERY, window_size=100, mode='ltr'),
        "ES_ltr_ws20_query2": lambda query: cached_search(es, index, query, es_query=LTR_QUERY2, window_size=20,
                                                          mode='ltr'),
        "ES_ltr_ws20_query2b": lambda query: cached_search(es, index, query, es_query=LTR_QUERY2b, window_size=20,
                                                           mode='ltr'),
        "API_ltr_nNone_tNone": lambda query: cached_search_with_latency(query, host=nghost, port=ngport,
                                                                        name_diversity_ratio=None,
                                                                        max_per_type=None, sort_order='AI'),
        "API_ltr_n0.5_t3": lambda query: cached_search_with_latency(query, host=nghost, port=ngport, sort_order='AI'),
    }
    endpoint_names = tuple(endpoints.keys())

    query2latencies = {query: {endpoint: [] for endpoint in endpoints} for query in queries}
    query2tooks = {query: {endpoint: [] for endpoint in endpoints} for query in queries}
    query2result = {query: {endpoint: [] for endpoint in endpoints} for query in queries}

    for _ in tqdm.tqdm(range(args.repeats), total=args.repeats, desc='repeats'):
        for endpoint, endpoint_func in tqdm.tqdm(endpoints.items(), desc=f'endpoints'):
            for query in tqdm.tqdm(queries, desc=f'iteration {_} window size {endpoint}'):
                result, latency, took = endpoint_func(query)
                query2latencies[query][endpoint].append(latency)
                query2tooks[query][endpoint].append(took)
                query2result[query][endpoint] = result
    #     f'<p>NDCG@3: {ndcg_at_3:.2f}, NDCG@10: {ndcg_at_10:.2f}, Recall@3: {recall_at_3:.2f}, Recall@10: {recall_at_10:.2f}, Binary NDCG@3: {binary_ndcg_at_3:.2f}, Binary NDCG@10: {binary_ndcg_at_10:.2f}</p>')

    metrics = {
        'NDCG@3': lambda true_scores, annotated_scores: ndcg(true_scores, annotated_scores, k=3),
        'NDCG@10': lambda true_scores, annotated_scores: ndcg(true_scores, annotated_scores, k=10),
        'Binary NDCG@3': lambda true_scores, annotated_scores: ndcg(binarize_score(true_scores),
                                                                    binarize_score(annotated_scores), k=3,
                                                                    default_score=0.0),
        'Binary NDCG@10': lambda true_scores, annotated_scores: ndcg(binarize_score(true_scores),
                                                                     binarize_score(annotated_scores), k=10,
                                                                     default_score=0.0),
        'Recall@3': lambda true_scores, annotated_scores: sum(
            [1 for true_score in true_scores[:3] if true_score > 4]) / 3,
        'Recall@10': lambda true_scores, annotated_scores: sum(
            [1 for true_score in true_scores[:10] if true_score > 4]) / 10,
        'NDCG@3 def5': lambda true_scores, annotated_scores: ndcg(true_scores, annotated_scores, k=3, default_score=5),
        'NDCG@10 def5': lambda true_scores, annotated_scores: ndcg(true_scores, annotated_scores, k=10,
                                                                   default_score=5),
    }
    query2metric = {metric: {query: {endpoint: 0.0 for endpoint in endpoint_names} for query in queries} for metric in
                    metrics}
    # calculating metrics
    # query2ndcg_at_3 = {query: {endpoint: 0.0 for endpoint in endpoint_names} for query in queries}
    # query2ndcg_at_10 = {query: {endpoint: 0.0 for endpoint in endpoint_names} for query in queries}
    # query2binary_ndcg_at_3 = {query: {endpoint: 0.0 for endpoint in endpoint_names} for query in queries}
    # query2binary_ndcg_at_10 = {query: {endpoint: 0.0 for endpoint in endpoint_names} for query in queries}
    # query2recall_at_3 = {query: {endpoint: 0.0 for endpoint in endpoint_names} for query in queries}
    # query2recall_at_10 = {query: {endpoint: 0.0 for endpoint in endpoint_names} for query in queries}

    for query in queries:
        for endpoint in endpoint_names:
            result = query2result[query][endpoint]
            # collection_names = [hit['fields']['data.collection_name'][0] for hit in result['hits']['hits']]
            collection_names = [c.title for c in result]

            # print(query, collection_names)
            # scores = [hit['_score'] for hit in result['hits']['hits']]
            true_scores = [human_scores[query].get(collection_name, 2) for collection_name in collection_names]
            annotated_scores = human_scores[query].values()

            # binarized_scores = [1 if score >= 4 else 0 for score in true_scores]
            # binarized_annotated_scores = [1 if score >= 4 else 0 for score in annotated_scores]

            # print(scores, true_scores)
            # print(query)
            if not collection_names:
                print('  no results')
            #     print()
            #     ndcg_at_3 = 0.0
            #     ndcg_at_10 = 0.0
            #     binary_ndcg_at_3 = 0.0
            #     binary_ndcg_at_10 = 0.0
            #     recall_at_3 = 0.0
            #     recall_at_10 = 0.0
            # else:
            #     ndcg_at_3 = ndcg(true_scores, annotated_scores, k=3)
            #     ndcg_at_10 = ndcg(true_scores, annotated_scores, k=10)
            #     print(query, true_scores, annotated_scores, ndcg_at_10)
            #     binary_ndcg_at_3 = ndcg(binarized_scores, binarized_annotated_scores, k=3, default_score=0.0)
            #     binary_ndcg_at_10 = ndcg(binarized_scores, binarized_annotated_scores, k=10, default_score=0.0)
            #     recall_at_3 = sum([1 for collection_name in collection_names[:3]
            #                        if human_scores[query].get(collection_name, 2) > 4]) / 3
            #     recall_at_10 = sum([1 for collection_name in collection_names[:10]
            #                         if human_scores[query].get(collection_name, 2) > 4]) / 10
            # 
            # query2ndcg_at_3[query][endpoint] = ndcg_at_3
            # query2ndcg_at_10[query][endpoint] = ndcg_at_10
            # query2binary_ndcg_at_3[query][endpoint] = binary_ndcg_at_3
            # query2binary_ndcg_at_10[query][endpoint] = binary_ndcg_at_10
            # query2recall_at_3[query][endpoint] = recall_at_3
            # query2recall_at_10[query][endpoint] = recall_at_10

            for metric, metric_func in metrics.items():
                query2metric[metric][query][endpoint] = metric_func(true_scores, annotated_scores)

    with open(args.output, 'w') as f:
        # add style (borderless table, with enough padding, good font, colors and spacing)
        f.write(
            '<html><head><style>'
            'table {border-collapse: collapse; font-family: "Trebuchet MS", Arial, Helvetica, sans-serif; width: 100%;} '
            'td, th {border: 1px solid #ddd; padding: 8px;} '
            'tr:nth-child(even){background-color: #f2f2f2;} '
            'tr:hover {background-color: #ddd;} '
            'th {padding-top: 12px; padding-bottom: 12px; text-align: left; background-color: #4CAF50; color: white;}'
            'h2 {font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;} '
            '</style></head><body>'
        )

        f.write('<html><body><table border="1">')
        # write both only LTR
        f.write(
            '<tr>'
            '<th>Query</th>'
            '<th></th>'
            + ''.join(f'<th>Latency (ms)\nWindow Size {endpoint}</th>' for endpoint in endpoint_names)
            + '<th></th>'
            + ''.join(f'<th>Took (ms)\nWindow Size {endpoint}</th>' for endpoint in endpoint_names)
            + '</tr>'
        )

        # only checking LTR
        for query in queries:
            f.write('<tr>')
            f.write(f'<td>{query}</td>')
            f.write(f'<td></td>')
            for endpoint in endpoint_names:
                f.write(
                    f'<td>{np.mean(query2latencies[query][endpoint]):.2f} ± {np.std(query2latencies[query][endpoint]):.2f}</td>')
            f.write('<td></td>')
            for endpoint in endpoint_names:
                f.write(
                    f'<td>{np.mean(query2tooks[query][endpoint]):.2f} ± {np.std(query2tooks[query][endpoint]):.2f}</td>')
            f.write('</tr>')

        f.write('</table></body></html>')

        # all queries aggregated
        f.write('<html><body><table border="1">')
        f.write(
            '<tr><th>Window Size</th><th>Latency (ms)</th><th>Took (ms)</th>')

        for metric_name in query2metric:
            f.write(f'<th>{metric_name}</th>')
        f.write('</tr>')

        for endpoint in endpoint_names:
            latency_times = [el for query in queries for el in query2latencies[query][endpoint]]
            took_times = [el for query in queries for el in query2tooks[query][endpoint]]

            f.write(
                f'<tr>'
                f'<td>{endpoint}</td>'
                f'<td>{np.mean(latency_times):.1f} ± {np.std(latency_times):.1f}</td>'
                f'<td>{np.mean(took_times):.1f} ± {np.std(latency_times):.1f}</td>'
            )

            for metric_name, metric_values in query2metric.items():
                values = [metric_values[query][endpoint] for query in queries]
                f.write(f'<td>{np.mean(values):.3f} ± {np.std(values):.3f}</td>')

            f.write(
                f'</tr>'
            )
        f.write('</table></body></html>')

        from matplotlib.lines import Line2D

        markers = iter(Line2D.markers)

        fig, ax = plt.subplots(figsize=(18, 8))
        for metric_name, metric_values in query2metric.items():
            y = [np.mean([metric_values[query][endpoint] for query in queries]) for endpoint in
                 endpoint_names]
            ax.plot(endpoint_names, y, label=metric_name, marker=next(markers))
        ax.grid(which='major', alpha=0.5)
        ax.grid(which='minor', alpha=0.2)
        ax.set_yticks(np.arange(0, 1.1, 0.1))
        plt.legend()
        plt.xticks(rotation=15)
        plt.tight_layout()

        encoded = fig_to_base64(fig)
        f.write('<img src="data:image/png;base64, {}">'.format(encoded.decode('utf-8')))

        y = [np.mean([query2tooks[query][endpoint] for query in queries]) for endpoint in
             endpoint_names]
        fig, ax = plt.subplots(figsize=(18, 5))
        ax.plot(endpoint_names, y, c='forestgreen')
        ax.scatter(endpoint_names, y, c='forestgreen')
        ax.set_title('Took')
        # ax.set_xlabel('Window Size')
        # ax11.set_xticks(major_ticks)
        # ax11.set_xticks(minor_ticks, minor=True)
        ax.grid(which='major', alpha=0.5)
        ax.grid(which='minor', alpha=0.2)
        plt.xticks(rotation=15)
        plt.tight_layout()

        encoded = fig_to_base64(fig)
        f.write('<img src="data:image/png;base64, {}">'.format(encoded.decode('utf-8')))

        for metric_name, metric_values in query2metric.items():
            y = [np.mean([metric_values[query][endpoint] for query in queries]) for endpoint in
                 endpoint_names]
            fig, ax = plt.subplots(figsize=(18, 5))
            ax.plot(endpoint_names, y, c='forestgreen')
            ax.scatter(endpoint_names, y, c='forestgreen')
            ax.set_title(metric_name)
            # ax.set_xlabel('Window Size')
            # ax11.set_xticks(major_ticks)
            # ax11.set_xticks(minor_ticks, minor=True)
            ax.grid(which='major', alpha=0.5)
            ax.grid(which='minor', alpha=0.2)
            plt.xticks(rotation=15)
            plt.tight_layout()

            encoded = fig_to_base64(fig)
            f.write('<img src="data:image/png;base64, {}">'.format(encoded.decode('utf-8')))

        for query in queries:
            f.write(f'<h1>{query}</h1>')
            for endpoint in endpoint_names:
                result = query2result[query][endpoint]
                f.write(f'<h2>{endpoint}</h2>')
                # f.write('<ol>')
                # for collection in result:
                #     f.write(
                #         f'<li>{collection.avatar_emoji} {collection.title} {collection.types} {collection.number_of_names} {collection.names}</li>')
                # f.write('</ol>')

                # f.write(
                #     f'<p>NDCG@3: {ndcg_at_3:.2f}, NDCG@10: {ndcg_at_10:.2f}, Recall@3: {recall_at_3:.2f}, Recall@10: {recall_at_10:.2f}, Binary NDCG@3: {binary_ndcg_at_3:.2f}, Binary NDCG@10: {binary_ndcg_at_10:.2f}</p>')

                f.write('<p>')
                for metric_name in query2metric:
                    f.write(f'{metric_name}: {query2metric[metric_name][query][endpoint]:.2f}, ')
                f.write('</p>')

                f.write('<table>')
                for collection in result:
                    types = ', '.join(collection.types)
                    names = ', '.join([n.replace('.eth', '') for n in collection.names])

                    human_score = human_scores[query].get(collection.title, '')
                    f.write(
                        f'<tr>'
                        f'<td>{human_score}</td>'
                        f'<td>{collection.avatar_emoji}</td>'
                        f'<td>{collection.title}</td>'
                        f'<td>{types}</td>'
                        f'<td>{collection.number_of_names}</td>'
                        f'<td>{names}</td>'
                        f'</tr>'
                    )
                f.write('</table>')
