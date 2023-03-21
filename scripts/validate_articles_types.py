import collections
from argparse import ArgumentParser

import more_itertools
from SPARQLWrapper import SPARQLWrapper, JSON
from wikimapper import WikiMapper
import jsonlines as jsonlines
from tqdm import tqdm

CORRECT = 'correct'
INCORRECT = 'incorrect'

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")


def validate_articles(type_id, articles):
    correct = []
    incorrect = []

    # convert article names to wikidata id
    members_ids = {}
    for member in articles:
        wikidata_id = mapper.title_to_id(member.replace(' ', '_'))
        # print(member)
        members_ids[member] = wikidata_id

    correct_ids = set([id_link.split('/')[-1] for id_link in validate_wikidata_ids(type_id, members_ids.values())])
    # print('correct_ids', correct_ids)

    # print(members_ids)
    for article, id in members_ids.items():
        if id in correct_ids:
            correct.append(article)
        else:
            incorrect.append(article)

    # print('correct', correct)
    # print('incorrect', incorrect)
    # print()

    return correct, incorrect


def validate_wikidata_ids(type_id, ids):
    members_str = ''.join([f'(wd:{id})' for id in ids])
    query = """SELECT DISTINCT ?item WHERE {{
  VALUES (?item) {{{members_str}}}
  ?item wdt:P31*/wdt:P279* wd:{type_id}
}}""".format(type_id=type_id, members_str=members_str)

    # much slower
    # members_str2 = ','.join([f'wd:{id}' for id in ids])
    # query2 = """SELECT DISTINCT ?item WHERE {{
    #   ?item wdt:P31*/wdt:P279* wd:{type_id}
    #   FILTER (?item IN ({members_str}))
    # }}""".format(type_id=type_id, members_str=members_str2)
    # 
    # print(query2)
    # return

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    # print(query)

    res = [item['item']['value'] for item in results['results']['bindings']]
    return res


if __name__ == '__main__':
    parser = ArgumentParser(description='Too slow - 1s per article. NOT USED')
    parser.add_argument('inputs', nargs='+', help='JSONL files with category/list members')
    parser.add_argument('-o', '--output', help='JSONL file with types and validated articles')
    args = parser.parse_args()

    validated_types_articles = {}
    try:
        with jsonlines.open(args.output) as reader:
            for data in reader:
                type = data['type']
                correct = data[CORRECT]
                incorrect = data[INCORRECT]
                if type not in validated_types_articles:
                    validated_types_articles[type] = {}
                    validated_types_articles[type][CORRECT] = set(correct)
                    validated_types_articles[type][INCORRECT] = set(incorrect)
                else:
                    validated_types_articles[type][CORRECT] |= set(correct)
                    validated_types_articles[type][INCORRECT] |= set(incorrect)
            print(f'Saved {len(validated_types_articles)}, continuing')
    except FileNotFoundError:
        pass

    all_members = []
    type_members = collections.defaultdict(set)

    for path in args.inputs:
        with jsonlines.open(path) as reader:
            for obj in tqdm(reader):
                type = obj['type']
                members = [x[-1] for x in obj['members']]
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

    mapper = WikiMapper("data/index_enwiki-latest.db")

    with jsonlines.open(args.output, mode='a') as writer:
        for type, members in tqdm(type_members.items()):
            print(type, list(members)[:5])
            type_id = type.split('/')[-1]

            # check only not validated yet
            try:
                members -= validated_types_articles[type][CORRECT]
            except KeyError:
                pass
            try:
                members -= validated_types_articles[type][INCORRECT]
            except KeyError:
                pass

            # validate
            for members_batch in tqdm(list(more_itertools.chunked(members, 10)), leave=False):
                correct, incorrect = validate_articles(type_id, members_batch)
                # save batch
                writer.write({'type': type, CORRECT: correct, INCORRECT: incorrect})
