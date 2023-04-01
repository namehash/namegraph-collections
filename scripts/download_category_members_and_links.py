import time
import traceback
import urllib
from argparse import ArgumentParser
import json
import jsonlines

import tqdm

from scripts.functions import WikiAPI


# def category_members2(category_name: str) -> list:
#     """Uses Wikidata sparql, might have worse API limits but returns Wikidata id."""
#     a = """SELECT ?item ?pageid ?title WHERE {{
#       SERVICE wikibase:mwapi {{
#          bd:serviceParam wikibase:api "Generator" .
#          bd:serviceParam wikibase:endpoint "en.wikipedia.org" .
#          bd:serviceParam mwapi:gcmtitle '{category_name}' .
#          bd:serviceParam mwapi:generator "categorymembers" .
#          bd:serviceParam mwapi:gcmprop "ids|title|type" .
#          bd:serviceParam mwapi:gcmlimit "max" .
#         ?item wikibase:apiOutputItem mwapi:item .
#                  ?pageid wikibase:apiOutput "@pageid" .
#          ?title wikibase:apiOutput "@title" .
#       }}
#     }}""".format(category_name=category_name, type=type)
# 
#     sparql.setQuery(a)
#     sparql.setReturnFormat(JSON)
#     results = sparql.query().convert()
# 
#     res = [item['itemLabel']['value'] for item in results['results']['bindings']]
#     return res


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='JSON file with categories')
    parser.add_argument('output', help='JSONL file for the output collections')
    parser.add_argument('--mode', default='category', choices=['category', 'list'], help='mode')
    args = parser.parse_args()

    wiki_api = WikiAPI()

    if args.mode == 'category':
        func = wiki_api.category_members
    elif args.mode == 'list':
        func = wiki_api.links

    with open(args.input, 'r', encoding='utf-8') as f:
        lists = json.load(f)

    saved_items = set()
    try:
        with jsonlines.open(args.output) as reader:
            for wikicategory in reader:
                saved_items.add(wikicategory['article'])
            print(f'Saved {len(saved_items)}, continuing')
    except FileNotFoundError:
        pass

    with jsonlines.open(args.output, mode='a') as writer:
        for wikicategory in tqdm.tqdm(lists):
            article = wikicategory['article']
            if article in saved_items: continue

            en_label = article.split('/')[-1]

            try:
                # time.sleep(0.1)
                members = func(en_label)
                wikicategory['members'] = members
                writer.write(wikicategory)

            except KeyError as e:
                print(e)
                print(en_label)
                traceback.print_exc()
            except json.decoder.JSONDecodeError as e:
                print(e)
                print(en_label)
                traceback.print_exc()
                # also throttling
                time.sleep(5)
            except urllib.error.HTTPError as e:
                print(e)
                print(en_label)
                traceback.print_exc()
                time.sleep(5)
            except Exception as e:
                print(e)
                print(en_label)
                traceback.print_exc()
                time.sleep(5)
