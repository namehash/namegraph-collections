import csv
import json
import time
import traceback
import urllib
from argparse import ArgumentParser

import jsonlines as jsonlines
import regex
from ens_normalize import ens_force_normalize, DisallowedLabelError
from tqdm import tqdm

from scripts.functions import WikiAPI

if __name__ == '__main__':
    parser = ArgumentParser(description='')
    parser.add_argument('qrank', help='CSV from https://qrank.wmcloud.org/')
    parser.add_argument('output', help='JSONL file with collections')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')

    args = parser.parse_args()

    wiki_api = WikiAPI()

    ranks = []
    with open(args.qrank, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        try:
            for id, rank in tqdm(reader):
                ranks.append((int(rank), id))
        except ValueError:
            for (id,) in tqdm(reader):
                # print(id)
                ranks.append((1, id))

    # ranks = list(sorted(ranks, key=lambda x: x[0], reverse=True))

    saved_items = set()
    try:
        with jsonlines.open(args.output) as reader:
            for data in reader:
                saved_items.add(data['id'])
            print(f'Saved {len(saved_items)}, continuing')
    except FileNotFoundError:
        pass

    with jsonlines.open(args.output, mode='a') as writer:
        for rank, id in tqdm(ranks):
            if id in saved_items:
                continue
            for star in [True, False]:
                try:
                    # print(rank, id)
                    time.sleep(0.1)
                    instances = wiki_api.get_instances(id, star=star)
                    writer.write({'id': id, 'rank': rank, 'all': star, 'instances': instances})
                    break
                except KeyError as e:
                    print(e)
                    print(rank, id)
                    traceback.print_exc()
                except json.decoder.JSONDecodeError as e:
                    print(e)
                    print(rank, id)
                    if 'TimeoutException' in e.msg:
                        print('TIMEOUT detected')
                        writer.write({'id': id, 'rank': rank, 'all': star, 'instances': [], 'timeout': True})
                    traceback.print_exc()
                    # also throttling
                    time.sleep(5)
                except urllib.error.HTTPError as e:
                    print(e)
                    print(rank, id)
                    traceback.print_exc()
                    time.sleep(5)
                except Exception as e:
                    print(e)
                    print(rank, id)
                    traceback.print_exc()
                    time.sleep(5)
