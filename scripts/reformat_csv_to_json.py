import sys
from argparse import ArgumentParser
from urllib.parse import unquote

import jsonlines
import json
import csv

from tqdm import tqdm

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='csv input filepath')
    parser.add_argument('output', help='output json filepath')
    parser.add_argument('--list_of_collections', help='json file containing all categories/lists')
    # parser.add_argument('--mode', default='category', choices=['category', 'list'], help='mode')
    parser.add_argument('--grouping_batch_size', default=1_000_000, type=int, help='batch size for the grouping')
    args = parser.parse_args()

    with open(args.list_of_collections, 'r', encoding='utf-8') as f:
        list_of_collections = json.load(f)
        collections = {
            unquote(coll['article'].removeprefix('Category:')): coll
            for coll in list_of_collections
        }

    with open(args.input, 'r', encoding='utf-8') as csvfile, jsonlines.open(args.output, 'w') as writer:
        reader = csv.reader(csvfile, delimiter=',')

        header = next(reader)
        first_row = next(reader)

        prev_key = first_row[0]
        members = [first_row[1]]
        for key, member in tqdm(reader):
            if key != prev_key:
                try:
                    item = collections[prev_key]
                    writer.write({
                        'item': item['item'],
                        'type': item['type'],
                        'article': item['article'],
                        'members': [m.replace('_', ' ') for m in members]
                    })
                except KeyError as ex:
                    print('Missing:', prev_key, file=sys.stderr)

                prev_key = key
                members = []

            members.append(member)

        # adding the last collection
        try:
            item = collections[key]
            writer.write({
                'item': item['item'],
                'type': item['type'],
                'article': item['article'],
                'members': [m.replace('_', ' ') for m in members]
            })
        except KeyError as ex:
            print('Missing:', key, file=sys.stderr)