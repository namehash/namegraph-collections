from argparse import ArgumentParser
from copy import deepcopy
import json
import tqdm
import time
import os

import numpy as np
from elasticsearch import Elasticsearch


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
               'metadata.modified'],
    "size": 10,
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


def search(es: Elasticsearch, index: str, query: str, mode: str, size=10) -> tuple[dict, float, float]:
    if mode == 'ltr':
        query_body = deepcopy(LTR_QUERY)
        query_body['rescore']['query']['rescore_query']['sltr']['params']['keywords'] = query
    else:
        query_body = deepcopy(NO_LTR_QUERY)

    query_body['query']['bool']['must'][0]['multi_match']['query'] = query

    t0 = time.perf_counter()
    result = es.search(
        index=index,
        body=query_body,
        size=size,
    )
    t1 = time.perf_counter()

    return result, (t1 - t0) * 1000, result['took']


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
    query2result = {query: {'ltr': [], 'no-ltr': []} for query in queries}

    for _ in range(args.repeats):
        for mode in ['ltr', 'no-ltr']:
            for query in tqdm.tqdm(queries):
                result, latency, took = search(es, args.index, query, mode)
                query2latencies[query][mode].append(latency)
                query2tooks[query][mode].append(took)
                query2result[query][mode] = result

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
        # write both LTR and no-LTR times for each query in one row
        f.write(
            '<tr>'
            '<th>Query</th>'
            '<th>Latency (ms) LTR</th>'
            '<th>Took (ms) LTR</th>'
            '<th>Latency (ms) no-LTR</th>'
            '<th>Took (ms) no-LTR</th>'
            '</tr>'
        )
        for query in queries:
            # use bold to highlight the corresponding best time
            ltr_latency_modifier = ' style="font-weight:bold"' if np.mean(query2latencies[query]['ltr']) < np.mean(
                query2latencies[query]['no-ltr']) else ''
            ltr_took_modifier = ' style="font-weight:bold"' if np.mean(query2tooks[query]['ltr']) < np.mean(
                query2tooks[query]['no-ltr']) else ''
            no_ltr_latency_modifier = ' style="font-weight:bold"' if np.mean(query2latencies[query]['ltr']) > np.mean(
                query2latencies[query]['no-ltr']) else ''
            no_ltr_took_modifier = ' style="font-weight:bold"' if np.mean(query2tooks[query]['ltr']) > np.mean(
                query2tooks[query]['no-ltr']) else ''

            f.write(
                f'<tr>'
                f'<td>{query}</td>'
                f'<td {ltr_latency_modifier}>{np.mean(query2latencies[query]["ltr"]):.1f} ± {np.std(query2latencies[query]["ltr"]):.1f}</td>'
                f'<td {ltr_took_modifier}>{np.mean(query2tooks[query]["ltr"]):.1f} ± {np.std(query2tooks[query]["ltr"]):.1f}</td>'
                f'<td {no_ltr_latency_modifier}>{np.mean(query2latencies[query]["no-ltr"]):.1f} ± {np.std(query2latencies[query]["no-ltr"]):.1f}</td>'
                f'<td {no_ltr_took_modifier}>{np.mean(query2tooks[query]["no-ltr"]):.1f} ± {np.std(query2tooks[query]["no-ltr"]):.1f}</td>'
                f'</tr>'
            )
        f.write('</table></body></html>')

        # all queries aggregated
        f.write('<html><body><table border="1">')
        f.write('<tr><th>Mode</th><th>Latency (ms)</th><th>Took (ms)</th></tr>')
        for mode in ['ltr', 'no-ltr']:
            latency_times = [el for query in queries for el in query2latencies[query][mode]]
            took_times = [el for query in queries for el in query2tooks[query][mode]]

            mode_str = 'LTR' if mode == 'ltr' else 'no-LTR'

            f.write(
                f'<tr>'
                f'<td>{mode_str}</td>'
                f'<td>{np.mean(latency_times):.1f} ± {np.std(latency_times):.1f}</td>'
                f'<td>{np.mean(took_times):.1f} ± {np.std(latency_times):.1f}</td>'
                f'</tr>'
            )
        f.write('</table></body></html>')

        # results per query
        for query in queries:
            ltr_result = query2result[query]['ltr']['hits']['hits']
            no_ltr_result = query2result[query]['no-ltr']['hits']['hits']

            # write the query and the table with the results
            # (featuring the collection name, the score, and names inside the collection)
            f.write(f'<h2>{query}</h2>')
            f.write('<html><body><table border="1">')

            ltr_latency_times = [el for query in queries for el in query2latencies[query]['ltr']]
            ltr_took_times = [el for query in queries for el in query2tooks[query]['ltr']]

            no_ltr_latency_times = [el for query in queries for el in query2latencies[query]['no-ltr']]
            no_ltr_took_times = [el for query in queries for el in query2tooks[query]['no-ltr']]

            ltr_times = f'latency: {np.mean(ltr_latency_times):.1f} ± {np.std(ltr_latency_times):.1f}ms | ' \
                        f'took: {np.mean(ltr_took_times):.1f} ± {np.std(ltr_took_times):.1f}ms'

            no_ltr_times = f'latency: {np.mean(no_ltr_latency_times):.1f} ± {np.std(no_ltr_latency_times):.1f}ms | ' \
                            f'took: {np.mean(no_ltr_took_times):.1f} ± {np.std(no_ltr_took_times):.1f}ms'

            f.write(
                f'<tr>'
                f'<th colspan="4" style="text-align:center">LTR<br>{ltr_times}</th>'
                f'<th></td>'
                f'<th colspan="4" style="text-align:center">no-LTR<br>{no_ltr_times}</th>'
                f'</tr>'
            )
            f.write(
                '<tr>'
                '<th>Score</th><th>Collection</th><th>Type</th><th>Names</th>'
                '<th></th>'
                '<th>Score</th><th>Collection</th><th>Type</th><th>Names</th>'
                '</tr>'
            )

            for ltr, no_ltr in zip(ltr_result, no_ltr_result):
                ltr_score = ltr['_score']
                ltr_collection = ltr['fields']['data.collection_name'][0]
                ltr_type = ', '.join([t for t in ltr['fields']['template.collection_types'][1::2]])
                ltr_names = ', '.join(ltr['fields']['template.top10_names.normalized_name'])

                no_ltr_score = no_ltr['_score']
                no_ltr_collection = no_ltr['fields']['data.collection_name'][0]
                no_ltr_type = ', '.join([t for t in no_ltr['fields']['template.collection_types'][1::2]])
                no_ltr_names = ', '.join(no_ltr['fields']['template.top10_names.normalized_name'])

                f.write(
                    f'<tr>'
                    f'<td>{ltr_score}</td>'
                    f'<td>{ltr_collection}</td>'
                    f'<td>{ltr_type}</td>'
                    f'<td>{ltr_names}</td>'
                    f'<td></td>'
                    f'<td>{no_ltr_score}</td>'
                    f'<td>{no_ltr_collection}</td>'
                    f'<td>{no_ltr_type}</td>'
                    f'<td>{no_ltr_names}</td>'
                    f'</tr>'
                )
            f.write('</table></body></html>')
