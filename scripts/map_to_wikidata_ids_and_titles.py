from argparse import ArgumentParser
import json
import csv

import wikimapper


def category_title2wikidata_id(title: str) -> str:
    return categories_mapping['Category:' + title]['item']


def wikipedia_id2title(wikipedia_id: int) -> str:
    if (title := cached_wikipedia_id2title.get(wikipedia_id)) is None:
        title = mapper.wikipedia_id_to_title(wikipedia_id)
        cached_wikipedia_id2title[wikipedia_id] = title

    return title


def wikipedia_id2wikidata_id(wikipedia_id: int) -> str:
    if (wikidata_id := cached_wikipedia_id2wikidata_id.get(wikipedia_id)) is None:
        wikidata_id = mapper.wikipedia_id_to_id(wikipedia_id)
        cached_wikipedia_id2wikidata_id[wikipedia_id] = wikidata_id

    return wikidata_id


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='csv input filepath')
    parser.add_argument('wikimapper', help='path to the wikimapper db')
    parser.add_argument('output', help='csv output filepath')
    parser.add_argument('--mode', default='category', choices=['category', 'list'], help='mode')
    parser.add_argument('--categories', default=None,
                        help='list to the categories JSON file featuring wikidata id and title')
    args = parser.parse_args()

    if args.mode == 'category' and args.categories is None:
        raise ValueError('if mode is `category`, then you must pass `categories` argument too')

    mapper = wikimapper.WikiMapper(args.wikimapper)
    cached_wikipedia_id2title: dict[int, str] = dict()
    cached_wikipedia_id2wikidata_id: dict[int, str] = dict()

    if args.mode == 'category':
        with open(args.categories, 'r', encoding='utf-8') as f:
            categories = json.load(f)
        categories_mapping = {
            category['article']: category
            for category in categories
        }

    skipped = 0
    with open(args.input, 'r', encoding='utf-8') as in_csv, open(args.output, 'w', encoding='utf-8') as out_csv:
        reader = csv.reader(in_csv, delimiter=',')
        writer = csv.writer(out_csv, delimiter=',')

        header = next(reader)
        writer.writerow(['collection_wikidata_id', 'member_title'])

        for line in reader:
            if args.mode == 'category':
                category_title = line[1]
                member_wikipedia_id = int(line[0])

                collection_wikidata_id = category_title2wikidata_id(category_title)
                member_title = wikipedia_id2title(member_wikipedia_id)

            elif args.mode == 'list':
                list_wikipedia_id = int(line[0])
                member_title = line[1]

                collection_wikidata_id = wikipedia_id2wikidata_id(list_wikipedia_id)
                member_title = member_title

            else:
                raise ValueError(f'invalid mode - {args.mode}')

            if collection_wikidata_id and member_title:
                writer.writerow([collection_wikidata_id, member_title])
            else:
                skipped += 1
                # FIXME no title is mapped!!!
                # print(collection_wikidata_id, member_wikidata_id, line)

    print('skipped', skipped)
