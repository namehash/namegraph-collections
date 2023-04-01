import json

import time
import traceback

from argparse import ArgumentParser

import jsonlines as jsonlines
from tqdm import tqdm

from scripts.functions import WikiAPI

CORRECT = 'correct'
INCORRECT = 'incorrect'

# sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
# sparql.setMethod('POST')

# def validate_types(type_id: str, article_types_ids: list[str], forward=False):
#     correct = []
#     incorrect = []
# 
#     if forward:
#         func = lambda id: validate_wikidata_ids(id, article_types_ids)
#     else:
#         func = get_subclasses
# 
#     correct_ids = set([id_link.split('/')[-1] for id_link in func(type_id)])
#     # print('correct_ids', correct_ids)
# 
#     # print(members_ids)
#     for id in article_types_ids:
#         if id in correct_ids:
#             correct.append(id)
#         else:
#             incorrect.append(id)
# 
#     # print('correct', correct)
#     # print('incorrect', incorrect)
#     # print()
# 
#     return correct, incorrect
# 
# 
# def get_subclasses(type_id: str):
#     query = """SELECT DISTINCT ?item WHERE {{
#       ?item wdt:P279* wd:{type_id}
#     }}""".format(type_id=type_id)
# 
#     sparql.setQuery(query)
#     sparql.setReturnFormat(JSON)
#     results = sparql.query().convert()
# 
#     res = [item['item']['value'] for item in results['results']['bindings']]
#     return res
# 
# 
# def validate_wikidata_ids(type_id: str, ids: list[str]):
#     members_str = ''.join([f'(wd:{id})' for id in ids])
#     query = """SELECT DISTINCT ?item WHERE {{
#   VALUES (?item) {{{members_str}}}
#   ?item wdt:P279* wd:{type_id}.
#   hint:Prior hint:gearing "forward".
# }}""".format(type_id=type_id, members_str=members_str)
# 
#     sparql.setQuery(query)
#     sparql.setReturnFormat(JSON)
#     results = sparql.query().convert()
# 
#     # print(query)
# 
#     res = [item['item']['value'] for item in results['results']['bindings']]
#     return res


if __name__ == '__main__':
    parser = ArgumentParser(description='Validate type pairs.')
    parser.add_argument('input', help='JSON file with types to validate')
    parser.add_argument('output', help='JSONL file with types and validated articles')
    args = parser.parse_args()

    type_members = json.load(open(args.input))

    validated_type_ids = {}
    try:
        with jsonlines.open(args.output) as reader:
            for data in reader:
                type_id = data['type']
                correct = data[CORRECT]
                incorrect = data[INCORRECT]
                validated_type_ids[type_id] = sorted(correct + incorrect)
            print(f'Saved {len(validated_type_ids)}, continuing')
    except FileNotFoundError:
        pass

    wiki_api = WikiAPI()

    with jsonlines.open(args.output, mode='a') as writer:
        for type, members in tqdm(type_members.items()):
            try:
                type_id = type.split('/')[-1]

                if type_id in validated_type_ids and sorted(members) == validated_type_ids[type_id]:
                    # print('cached')
                    continue
                # print('not cached', type_id in validated_type_ids, len(members), len(validated_type_ids[type_id]))
                # print(sorted(members), validated_type_ids[type_id])
                # continue

                print(len(members), type)
                print(members[:5])
                correct, incorrect = wiki_api.validate_types(type_id, members)
                print(len(correct), len(incorrect))

                writer.write({'type': type_id, CORRECT: correct, INCORRECT: incorrect})
            except Exception as e:
                print(e)
                traceback.print_exc()
                time.sleep(5)

                try:
                    correct, incorrect = wiki_api.validate_types(type_id, members, forward=True)
                    print(len(correct), len(incorrect))

                    writer.write({'type': type_id, CORRECT: correct, INCORRECT: incorrect})
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    time.sleep(5)
