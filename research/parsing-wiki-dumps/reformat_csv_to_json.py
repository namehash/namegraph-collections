from argparse import ArgumentParser
from typing import Iterable
from collections import defaultdict
import jsonlines
import json
import csv

from rocksdict import Rdict
from more_itertools import ichunked


def group_by(csvreader: Iterable[list], output: str, batch_size: int = 1_000_000):
    grouped = Rdict(output)

    try:
        for chunk in ichunked(csvreader, batch_size):
            # in-memory reversing
            mapping = defaultdict(list)
            for key, value in chunk:
                mapping[key].append(value)

            # writing to the memory
            for key, values in mapping.items():
                if key in grouped:
                    grouped[key] = grouped[key] + values
                else:
                    grouped[key] = values
    finally:
        grouped.close()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='csv input filepath')
    parser.add_argument('output', help='output json filepath')
    parser.add_argument('--list_of_collections', help='json file containing all categories/lists')
    parser.add_argument('--mode', default='category', choices=['category', 'list'], help='mode')
    parser.add_argument('--grouping_batch_size', default=1_000_000, type=int, help='batch size for the grouping')
    args = parser.parse_args()

    with open(args.list_of_collections, 'r', encoding='utf-8') as f:
        list_of_collections = json.load(f)
        collections = {
            coll['item']: coll
            for coll in list_of_collections
        }

    # FIXME key errors!!
    print(list(collections.keys())[:100])
    print('Q1009619' in collections)
    # exit()

    with open(args.input, 'r', encoding='utf-8') as csvfile, jsonlines.open(args.output, 'w') as writer:
        reader = csv.reader(csvfile, delimiter=',')

        header = next(reader)
        first_row = next(reader)

        prev_key = first_row[0]
        members = [first_row[1]]
        for key, member in reader:
            if key != prev_key:
                try:
                    item = collections[prev_key]
                    writer.write({
                        'item': item['item'],
                        'type': item['type'],
                        'article': item['article'],
                        'members': members
                    })
                except KeyError as ex:
                    print(prev_key)

                prev_key = key
                members = []

            members.append(member)

        # adding the last collection
        item = collections[key]
        writer.write({
            'item': item['item'],
            'type': item['type'],
            'article': item['article'],
            'members': members
        })


        # group_by(reader, 'grouped.rocksdb', batch_size=args.grouping_batch_size)
        #
        # rdict = Rdict('grouped.rocksdb')
        # for key, values in rdict.items():
        #     # TODO implement the rest
        #
        #     writer.write({
        #         'item': key,
        #         # 'type': [...],
        #         # 'article': ...,
        #         'members': values
        #     })
