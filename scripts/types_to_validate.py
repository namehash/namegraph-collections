import collections
import json

from argparse import ArgumentParser

import jsonlines as jsonlines
from tqdm import tqdm


def load_articles_types(path: str, verbose: bool = False) -> dict[str, set[str]]:
    articles_types = {}
    wo_types = 0
    wo_instanceof = 0
    wo_subclassof = 0
    with jsonlines.open(path) as reader:
        for obj in tqdm(reader):
            article = obj['article']
            instanceof = obj['instanceof']
            subclassof = obj['subclassof']
            if not instanceof:
                wo_instanceof += 1
            if not subclassof:
                wo_subclassof += 1

            types = set(instanceof + subclassof)
            if not types:
                wo_types += 1
            articles_types[article] = types

        if verbose:
            print('Articles', len(articles_types))
            print('Articles without instanceof and subclassof', wo_types)
            print('Articles without instanceof', wo_instanceof)
            print('Articles without subclassof', wo_subclassof)

    return articles_types


if __name__ == '__main__':
    parser = ArgumentParser(description='Generate pairs of types to be validated.')
    parser.add_argument('inputs', nargs='+', help='JSONL files with category/list members')
    parser.add_argument('-a', '--article_types', help='JSONL files with articletypes')
    parser.add_argument('-o', '--output', help='JSONL file with types and validated articles')
    args = parser.parse_args()

    articles_types = load_articles_types(args.article_types, verbose=True)

    # Articles 10334978
    # Articles without instanceof and subclassof 2011952
    # Articles without instanceof 2181368
    # Articles without subclassof 9886940
    
    validated_types_articles = {}

    all_members = []
    type_members = collections.defaultdict(set)
    for path in args.inputs:
        with jsonlines.open(path) as reader:
            for obj in tqdm(reader):
                type = obj['type']
                members = obj['members']

                for article in members:
                    article_types = articles_types[article]
                    all_members.extend(article_types)
                    type_members[type].update(article_types)

    print('Articles types', len(all_members))
    print('Unique articles types', len(set(all_members)))
    print('unique pairs: article type, category/link type', sum([len(v) for v in type_members.values()]))
    print('Unique types of categories/lists', len(type_members))

    # Articles types 69981781
    # Unique articles types 85585
    # unique pairs: article type, category/link type 1952768
    # Unique types of categories/lists 8594

    for key in list(type_members.keys()):
        type_members[key] = list(type_members[key])

    json.dump(type_members, open(args.output, 'w'))
