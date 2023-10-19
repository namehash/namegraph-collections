from argparse import ArgumentParser
from operator import itemgetter
import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def analyze_ranks(collection_ranks: dict[str, int], members_ranks: dict[str, dict[str, int]]):
    collection_ranks_series = pd.Series(collection_ranks)

    print('Collection ranks')
    print(collection_ranks_series.describe())
    print()

    print('Top 10 collection ranks')
    print(collection_ranks_series.nlargest(10))
    print()

    mean_members_ranks_series = pd.Series({
        collection_name: np.mean(list(members.values()))
        for collection_name, members in members_ranks.items()
    })

    max_members_ranks = dict()
    for collection_name, member_ranks in members_ranks.items():
        member, rank = max(member_ranks.items(), key=itemgetter(1))
        max_members_ranks[collection_name + ' / ' + member] = rank
    max_members_ranks_series = pd.Series(max_members_ranks)

    print('Mean members ranks')
    print(mean_members_ranks_series.describe())
    print()

    print('Top 10 mean members ranks')
    print(mean_members_ranks_series.nlargest(10))
    print()

    print('Max members ranks')
    print(max_members_ranks_series.describe())
    print()

    print('Top 10 max members ranks')
    print(max_members_ranks_series.nlargest(10))
    print()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--input', type=str, required=True, help='Paths to the input JSON file')
    args = parser.parse_args()

    with open(args.input, 'r', encoding='utf-8') as f:
        ranks = json.load(f)

    collection_ranks, members_ranks = ranks['collection_ranks'], ranks['members_ranks']
    analyze_ranks(collection_ranks, members_ranks)
