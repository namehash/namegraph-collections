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


def search(es: Elasticsearch, index: str, query: str, window_size: int, size: int = 10) -> tuple[dict, float, float]:
    mode = 'ltr'
    if mode == 'ltr':
        query_body = deepcopy(LTR_QUERY)
        query_body['rescore']['query']['rescore_query']['sltr']['params']['keywords'] = query
        query_body['rescore']['window_size'] = window_size
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


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--scores', help='file with human annotated scores')
    parser.add_argument('--output', default='window_sizes.html', help="file with the output report")
    parser.add_argument('--limit', default=10, type=int, help='limit the number of collections to retrieve')
    parser.add_argument('--repeats', default=10, type=int, help='number of times to repeat the query')
    parser.add_argument('--window_sizes', nargs='+', type=int, help='window sizes to test',
                        default=[1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 350, 500, 750, 1000])
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

    human_scores = dict()
    if args.scores:
        with open(args.scores) as input:
            for idx, row in enumerate(input):
                data = json.loads(row.strip())
                query = list(data.keys())[0]
                human_scores[query] = {}
                for result, score in data[query].items():
                    human_scores[query][result] = score

    queries = list(human_scores.keys())

    query2latencies = {query: {window_size: [] for window_size in args.window_sizes} for query in queries}
    query2tooks = {query: {window_size: [] for window_size in args.window_sizes} for query in queries}
    query2result = {query: {window_size: [] for window_size in args.window_sizes} for query in queries}

    for _ in range(args.repeats):
        for window_size in args.window_sizes:
            for query in tqdm.tqdm(queries, desc=f'iteration {_} window size {window_size}'):
                result, latency, took = search(es, index, query, window_size)
                query2latencies[query][window_size].append(latency)
                query2tooks[query][window_size].append(took)
                query2result[query][window_size] = result

    # calculating metrics
    query2ndcg_at_3 = {query: {window_size: 0.0 for window_size in args.window_sizes} for query in queries}
    query2ndcg_at_10 = {query: {window_size: 0.0 for window_size in args.window_sizes} for query in queries}
    query2binary_ndcg_at_3 = {query: {window_size: 0.0 for window_size in args.window_sizes} for query in queries}
    query2binary_ndcg_at_10 = {query: {window_size: 0.0 for window_size in args.window_sizes} for query in queries}
    query2recall_at_3 = {query: {window_size: 0.0 for window_size in args.window_sizes} for query in queries}
    query2recall_at_10 = {query: {window_size: 0.0 for window_size in args.window_sizes} for query in queries}

    for query in queries:
        for window_size in args.window_sizes:
            result = query2result[query][window_size]
            collection_names = [hit['fields']['data.collection_name'][0] for hit in result['hits']['hits']]

            # print(query, collection_names)
            # scores = [hit['_score'] for hit in result['hits']['hits']]
            true_scores = [human_scores[query].get(collection_name, 2) for collection_name in collection_names]
            annotated_scores = human_scores[query].values()

            binarized_scores = [1 if score >= 4 else 0 for score in true_scores]
            binarized_annotated_scores = [1 if score >= 4 else 0 for score in annotated_scores]

            # print(scores, true_scores)
            print(query)
            if not collection_names:
                print('  no results')
                print()
                ndcg_at_3 = 0.0
                ndcg_at_10 = 0.0
                binary_ndcg_at_3 = 0.0
                binary_ndcg_at_10 = 0.0
                recall_at_3 = 0.0
                recall_at_10 = 0.0
            else:
                ndcg_at_3 = ndcg(true_scores, annotated_scores, k=3)
                ndcg_at_10 = ndcg(true_scores, annotated_scores, k=10)
                binary_ndcg_at_3 = ndcg(binarized_scores, binarized_annotated_scores, k=3, default_score=0.0)
                binary_ndcg_at_10 = ndcg(binarized_scores, binarized_annotated_scores, k=10, default_score=0.0)
                recall_at_3 = sum([1 for collection_name in collection_names[:3]
                                   if human_scores[query].get(collection_name, 2) > 4]) / 3
                recall_at_10 = sum([1 for collection_name in collection_names[:10]
                                    if human_scores[query].get(collection_name, 2) > 4]) / 10

            query2ndcg_at_3[query][window_size] = ndcg_at_3
            query2ndcg_at_10[query][window_size] = ndcg_at_10
            query2binary_ndcg_at_3[query][window_size] = binary_ndcg_at_3
            query2binary_ndcg_at_10[query][window_size] = binary_ndcg_at_10
            query2recall_at_3[query][window_size] = recall_at_3
            query2recall_at_10[query][window_size] = recall_at_10

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
            + ''.join(f'<th>Latency (ms)\nWindow Size {window_size}</th>' for window_size in args.window_sizes)
            + '<th></th>'
            + ''.join(f'<th>Took (ms)\nWindow Size {window_size}</th>' for window_size in args.window_sizes)
            + '</tr>'
        )

        # only checking LTR
        for query in queries:
            f.write('<tr>')
            f.write(f'<td>{query}</td>')
            f.write(f'<td></td>')
            for window_size in args.window_sizes:
                f.write(f'<td>{np.mean(query2latencies[query][window_size]):.2f} ± {np.std(query2latencies[query][window_size]):.2f}</td>')
            f.write('<td></td>')
            for window_size in args.window_sizes:
                f.write(f'<td>{np.mean(query2tooks[query][window_size]):.2f} ± {np.std(query2tooks[query][window_size]):.2f}</td>')
            f.write('</tr>')

        f.write('</table></body></html>')

        # all queries aggregated
        f.write('<html><body><table border="1">')
        f.write('<tr><th>Window Size</th><th>Latency (ms)</th><th>Took (ms)</th><th>NDCG@3</th><th>NDCG@10</th><th>NDCG@3 (binarized)</th><th>NDCG@10 (binarized)</th><th>Recall@3</th><th>Recall@10</th></tr>')
        for window_size in args.window_sizes:
            latency_times = [el for query in queries for el in query2latencies[query][window_size]]
            took_times = [el for query in queries for el in query2tooks[query][window_size]]
            ndcgs_at_3 = [query2ndcg_at_3[query][window_size] for query in queries]
            ndcgs_at_10 = [query2ndcg_at_10[query][window_size] for query in queries]
            binary_ndcgs_at_3 = [query2binary_ndcg_at_3[query][window_size] for query in queries]
            binary_ndcgs_at_10 = [query2binary_ndcg_at_10[query][window_size] for query in queries]
            recalls_at_3 = [query2recall_at_3[query][window_size] for query in queries]
            recalls_at_10 = [query2recall_at_10[query][window_size] for query in queries]

            f.write(
                f'<tr>'
                f'<td>{window_size}</td>'
                f'<td>{np.mean(latency_times):.1f} ± {np.std(latency_times):.1f}</td>'
                f'<td>{np.mean(took_times):.1f} ± {np.std(latency_times):.1f}</td>'
                f'<td>{np.mean(ndcgs_at_3):.3f} ± {np.std(ndcgs_at_3):.3f}</td>'
                f'<td>{np.mean(ndcgs_at_10):.3f} ± {np.std(ndcgs_at_10):.3f}</td>'
                f'<td>{np.mean(binary_ndcgs_at_3):.3f} ± {np.std(binary_ndcgs_at_3):.3f}</td>'
                f'<td>{np.mean(binary_ndcgs_at_10):.3f} ± {np.std(binary_ndcgs_at_10):.3f}</td>'
                f'<td>{np.mean(recalls_at_3):.3f} ± {np.std(recalls_at_3):.3f}</td>'
                f'<td>{np.mean(recalls_at_10):.3f} ± {np.std(recalls_at_10):.3f}</td>'
                f'</tr>'
            )
        f.write('</table></body></html>')

        # plot mean ndcg@3 and ndcg@10, and recall@3 and recall@10 (x-axis: window size) - picture, not table
        fig, ((ax11, ax12), (ax21, ax22), (ax31, ax32)) = plt.subplots(3, 2, figsize=(30, 10))

        major_ticks = np.arange(0, max(args.window_sizes) + 1, 100)
        minor_ticks = np.arange(0, max(args.window_sizes) + 1, 20)

        y = [np.mean([query2ndcg_at_3[query][window_size] for query in queries]) for window_size in args.window_sizes]
        ax11.plot(args.window_sizes, y, c='forestgreen')
        ax11.scatter(args.window_sizes, y, c='forestgreen')
        ax11.set_title('NDCG@3')
        ax11.set_xlabel('Window Size')
        ax11.set_xticks(major_ticks)
        ax11.set_xticks(minor_ticks, minor=True)
        ax11.grid(which='major', alpha=0.5)
        ax11.grid(which='minor', alpha=0.2)

        y = [np.mean([query2ndcg_at_10[query][window_size] for query in queries]) for window_size in args.window_sizes]
        ax12.plot(args.window_sizes, y, c='forestgreen')
        ax12.scatter(args.window_sizes, y, c='forestgreen')
        ax12.set_title('NDCG@10')
        ax12.set_xlabel('Window Size')
        ax12.set_xticks(major_ticks)
        ax12.set_xticks(minor_ticks, minor=True)
        ax12.grid(which='major', alpha=0.5)
        ax12.grid(which='minor', alpha=0.2)

        y = [np.mean([query2recall_at_3[query][window_size] for query in queries]) for window_size in args.window_sizes]
        ax21.plot(args.window_sizes, y, c='forestgreen')
        ax21.scatter(args.window_sizes, y, c='forestgreen')
        ax21.set_title('Recall@3')
        ax21.set_xlabel('Window Size')
        ax21.set_xticks(major_ticks)
        ax21.set_xticks(minor_ticks, minor=True)
        ax21.grid(which='major', alpha=0.5)
        ax21.grid(which='minor', alpha=0.2)

        y = [np.mean([query2recall_at_10[query][window_size] for query in queries]) for window_size in args.window_sizes]
        ax22.plot(args.window_sizes, y, c='forestgreen')
        ax22.scatter(args.window_sizes, y, c='forestgreen')
        ax22.set_title('Recall@10')
        ax22.set_xlabel('Window Size')
        ax22.set_xticks(major_ticks)
        ax22.set_xticks(minor_ticks, minor=True)
        ax22.grid(which='major', alpha=0.5)
        ax22.grid(which='minor', alpha=0.2)

        y = [np.mean([query2ndcg_at_3[query][window_size] for query in queries]) for window_size in args.window_sizes]
        ax31.plot(args.window_sizes, y, c='forestgreen')
        ax31.scatter(args.window_sizes, y, c='forestgreen')
        ax31.set_title('NDCG@3 (binarized)')
        ax31.set_xlabel('Window Size')
        ax31.set_xticks(major_ticks)
        ax31.set_xticks(minor_ticks, minor=True)
        ax31.grid(which='major', alpha=0.5)
        ax31.grid(which='minor', alpha=0.2)

        y = [np.mean([query2binary_ndcg_at_10[query][window_size] for query in queries]) for window_size in args.window_sizes]
        ax32.plot(args.window_sizes, y, c='forestgreen')
        ax32.scatter(args.window_sizes, y, c='forestgreen')
        ax32.set_title('NDCG@10 (binarized)')
        ax32.set_xlabel('Window Size')
        ax32.set_xticks(major_ticks)
        ax32.set_xticks(minor_ticks, minor=True)
        ax32.grid(which='major', alpha=0.5)
        ax32.grid(which='minor', alpha=0.2)

        plt.tight_layout()

        encoded = fig_to_base64(fig)
        f.write('<img src="data:image/png;base64, {}">'.format(encoded.decode('utf-8')))
