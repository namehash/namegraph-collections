import collections
import time
import traceback
import urllib
from argparse import ArgumentParser

import more_itertools
import jsonlines as jsonlines
from tqdm import tqdm

from scripts.functions import WikiAPI

CORRECT = 'correct'
INCORRECT = 'incorrect'

if __name__ == '__main__':
    parser = ArgumentParser(description='For each Wikipedia article get instance of and subclass of.')
    parser.add_argument('inputs', nargs='+', help='JSONL files with category members')
    parser.add_argument('-o', '--output', help='JSONL file with types and validated articles')
    parser.add_argument('-b', '--batch_size', default=10000, type=int, help='batch size')
    parser.add_argument('-w', '--wiki_mapper', default='data/index_enwiki-latest.db',
                        help='path to WikiMapper database')
    args = parser.parse_args()

    processed_articles = set()
    try:
        with jsonlines.open(args.output) as reader:
            for data in reader:
                article = data['article']
                processed_articles.add(article)
            print(f'Saved {len(processed_articles)}, continuing')
    except FileNotFoundError:
        pass

    all_members = []
    type_members = collections.defaultdict(set)

    for path in args.inputs:
        with jsonlines.open(path) as reader:
            for obj in tqdm(reader):
                type = obj['type']
                members = obj['members']
                all_members.extend(members)
                type_members[type].update(members)

    print('Articles', len(all_members))
    print('Unique articles', len(set(all_members)))
    print('unique pairs: article, category type', sum([len(v) for v in type_members.values()]))
    print('Unique types of categories', len(type_members))

    # Articles 63748172
    # Unique articles 7169625
    # unique pairs: article, category type 9511466
    # Unique types of categories 9148

    all_members = list(set(all_members) - processed_articles)

    # mapper = WikiMapper(args.wiki_mapper)
    wiki_api = WikiAPI()
    wiki_api.init_wikimapper(args.wiki_mapper)

    with jsonlines.open(args.output, mode='a') as writer:
        for members_batch in tqdm(list(more_itertools.chunked(all_members, args.batch_size)), leave=False):
            try:
                time.sleep(1)
                types = wiki_api.get_types(members_batch)
                writer.write_all(types)
            except urllib.error.HTTPError as e:
                print(e)
                traceback.print_exc()
                time.sleep(5)
            except Exception as e:
                print(e)
                traceback.print_exc()
                time.sleep(5)
