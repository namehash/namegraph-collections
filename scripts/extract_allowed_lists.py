import sys
from argparse import ArgumentParser
import json
from urllib.parse import unquote

from wikimapper import WikiMapper

if __name__ == '__main__':
    parser = ArgumentParser(description='Extract allowed lists')
    parser.add_argument('lists', help='filepath to the JSON file')
    parser.add_argument('wikimapper', help='path to the wikimapper db')
    parser.add_argument('output', help='path for the output text file containing page ids')
    args = parser.parse_args()

    with open(args.lists, 'r', encoding='utf-8') as f:
        lists = json.load(f)

    wikimapper = WikiMapper(args.wikimapper)

    page_ids = []
    for list_obj in lists:
        wiki_id = wikimapper.title_to_wikipedia_id(unquote(list_obj['article']))
        if wiki_id is not None:
            page_ids.append(wiki_id)
        else:
            print('Missing', list_obj['article'], file=sys.stderr)
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write('\n'.join(map(str, page_ids)) + '\n')
