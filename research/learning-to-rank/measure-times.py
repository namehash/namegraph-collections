from argparse import ArgumentParser
from copy import deepcopy
import tqdm
import time
import os

import numpy as np
from elasticsearch import Elasticsearch


COMMON_QUERY = {
    "query": {
        "bool": {
            "must": [
                {
                    "multi_match": {
                        "fields": [
                            "data.collection_name",  # 1
                            "data.collection_name.exact",  # 2
                            "data.collection_description",  # 3
                            "data.collection_keywords",  # 4
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
                                     "boost": 1, }
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

    "rescore": {
        "window_size": 50,
        "query": {
            "rescore_query": {
                "sltr": {
                    "_name": "logged_featureset",
                    "store": "ltr-metadata-index",
                    "featureset": "ltr-feature-set",
                    "params": {
                        # "keywords": "rambo"
                    },
                }
            },
            "query_weight": 0
        }
    }
}


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


def search(es: Elasticsearch, index: str, query: str, mode: str, size=10) -> tuple[float, float]:
    query_body = deepcopy(COMMON_QUERY)
    query_body['query']['bool']['must'][0]['multi_match']['query'] = query
    if mode == 'ltr':
        query_body['rescore']['query']['rescore_query']['sltr']['params']['keywords'] = query
    else:
        query_body['query']['bool']['must'][0]['multi_match']['type'] = 'cross_fields'
        del query_body['rescore']

    t0 = time.perf_counter()
    result = es.search(
        index=index,
        query=query_body['query'],
        rescore=query_body.get('rescore'),
        size=size,
    )
    t1 = time.perf_counter()

    return (t1 - t0) * 1000, result['took']


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--queries', help='file with queries')
    parser.add_argument('--output', default='times.html', help="file with the output report")
    parser.add_argument('--index', default='collection-templates-1', help='index to search in')
    parser.add_argument('--limit', default=10, type=int, help='limit the number of collections to retrieve')
    parser.add_argument('--repeats', default=10, type=int, help='number of times to repeat the query')
    args = parser.parse_args()

    host = os.getenv('ES_HOST', 'localhost')
    port = int(os.getenv('ES_PORT', '9200'))
    username = os.getenv('ES_USERNAME', 'elastic')
    password = os.getenv('ES_PASSWORD', 'espass')

    es = connect_to_elasticsearch(
        scheme='http' if host in ['localhost', '127.0.0.1'] else 'https',
        host=host, port=port, username=username, password=password,
    )

    with open(args.queries) as f:
        queries = [line.strip() for line in f.readlines()]

    query2latencies = {query: {'ltr': [], 'no-ltr': []} for query in queries}
    query2tooks = {query: {'ltr': [], 'no-ltr': []} for query in queries}

    for _ in range(args.repeats):
        for mode in ['ltr', 'no-ltr']:
            for query in tqdm.tqdm(queries):
                latency, took = search(es, args.index, query, mode)
                query2latencies[query][mode].append(latency)
                query2tooks[query][mode].append(took)

    with open(args.output, 'w') as f:
        # add style (borderless table, with enough padding, good font, colors and spacing)
        f.write('<html><head><style>table {border-collapse: collapse; font-family: "Trebuchet MS", Arial, Helvetica, sans-serif; width: 100%;} td, th {border: 1px solid #ddd; padding: 8px;} tr:nth-child(even){background-color: #f2f2f2;} tr:hover {background-color: #ddd;} th {padding-top: 12px; padding-bottom: 12px; text-align: left; background-color: #4CAF50; color: white;}</style></head><body>')


        f.write('<html><body><table border="1">')
        # write both LTR and no-LTR times for each query in one row
        f.write('<tr><th>Query</th><th>Latency (ms) LTR</th><th>Took (ms) LTR</th><th>Latency (ms) no-LTR</th><th>Took (ms) no-LTR</th></tr>')
        for query in queries:
            f.write(f'<tr><td>{query}</td><td>{np.mean(query2latencies[query]["ltr"]):.1f} ± {np.std(query2latencies[query]["ltr"]):.1f}</td><td>{np.mean(query2tooks[query]["ltr"]):.1f} ± {np.std(query2tooks[query]["ltr"]):.1f}</td><td>{np.mean(query2latencies[query]["no-ltr"]):.1f} ± {np.std(query2latencies[query]["no-ltr"]):.1f}</td><td>{np.mean(query2tooks[query]["no-ltr"]):.1f} ± {np.std(query2tooks[query]["no-ltr"]):.1f}</td></tr>')
        f.write('</table></body></html>')

        # all queries aggregated
        f.write('<html><body><table border="1">')
        f.write('<tr><th>Mode</th><th>Latency (ms)</th><th>Took (ms)</th></tr>')
        for mode in ['ltr', 'no-ltr']:
            f.write(f'<tr><td>{mode}</td><td>{np.mean([np.mean(query2latencies[query][mode]) for query in queries]):.1f} ± {np.std([np.mean(query2latencies[query][mode]) for query in queries]):.1f}</td><td>{np.mean([np.mean(query2tooks[query][mode]) for query in queries]):.1f} ± {np.std([np.mean(query2tooks[query][mode]) for query in queries]):.1f}</td></tr>')
        f.write('</table></body></html>')
