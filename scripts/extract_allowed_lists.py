from argparse import ArgumentParser
import json

from wikimapper import WikiMapper


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('lists', help='filepath to the JSON file')
    parser.add_argument('wikimapper', help='path to the wikimapper db')
    parser.add_argument('output', help='path for the output text file containing page ids')
    args = parser.parse_args()

    with open(args.lists, 'r', encoding='utf-8') as f:
        lists = json.load(f)

    wikimapper = WikiMapper(args.wikimapper)

    page_ids = []
    for list_obj in lists:
        page_ids.extend(wikimapper.id_to_wikipedia_ids(list_obj['item']))

    with open(args.output, 'w', encoding='utf-8') as f:
        f.write('\n'.join(map(str, page_ids)) + '\n')
