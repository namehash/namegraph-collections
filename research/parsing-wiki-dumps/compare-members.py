from argparse import ArgumentParser
import jsonlines

import numpy as np


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input1', help='JSONL file')
    parser.add_argument('input2', help='JSONL file')
    args = parser.parse_args()

    collections1 = 0
    collections2 = 0
    lengths1 = []
    lengths2 = []
    wikidata_ids1 = []
    wikidata_ids2 = []

    with jsonlines.open(args.input1, 'r') as reader:
        for coll in reader:
            if not coll['members']:
                continue

            collections1 += 1
            wikidata_ids1.append(coll['item'])
            lengths1.append(len(coll['members']))

    with jsonlines.open(args.input2, 'r') as reader:
        for coll in reader:
            if not coll['members']:
                continue

            collections2 += 1
            wikidata_ids2.append(coll['item'])
            lengths2.append(len(coll['members']))

    print(f'{collections1=}')
    print(f'{collections2=}')
    print(f'mean members count 1: {np.mean(lengths1)}')
    print(f'mean members count 2: {np.mean(lengths2)}')

    ids1 = set(wikidata_ids1)
    ids2 = set(wikidata_ids2)
    print(f'first 30 present in 1, and not in 2: {list(ids1 - ids2)[30:60]}')
    print(f'first 30 present in 2, and not in 1: {list(ids2 - ids1)[30:60]}')
