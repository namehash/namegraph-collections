from argparse import ArgumentParser

import jsonlines as jsonlines
from tqdm import tqdm

from scripts.functions import WikiAPI
from types_to_validate import load_articles_types

CORRECT = 'correct'
INCORRECT = 'incorrect'


def load_validated_types(path: str) -> dict[str, dict[str, list[str]]]:
    validated_types = {}

    with jsonlines.open(path) as reader:
        for obj in tqdm(reader):
            type = obj['type']
            correct = set(obj[CORRECT])
            incorrect = set(obj[INCORRECT])

            validated_types[type] = {CORRECT: correct, INCORRECT: incorrect}
    return validated_types


if __name__ == '__main__':
    parser = ArgumentParser(description='Filter members using types')
    parser.add_argument('input', help='JSONL file with category/list members')
    parser.add_argument('article_types', help='JSONL file with types of articles')
    parser.add_argument('validated_types', help='JSONL file types validated with subclass of')
    parser.add_argument('output', help='JSONL file with validated category/list members')
    args = parser.parse_args()

    articles_types = load_articles_types(args.article_types)
    validated_types = load_validated_types(args.validated_types)

    count_valid_members = 0
    count_invalid_members = 0

    with jsonlines.open(args.input) as reader, jsonlines.open(args.output, mode='w') as writer:
        for obj in tqdm(reader):
            collection_item = obj['item']
            collection_type = obj['type']
            collection_type_id = WikiAPI.extract_id(collection_type)
            collection_article = obj['article']
            members = obj['members']

            collection_valid_subtypes = validated_types.get(collection_type_id, [])
            if not collection_valid_subtypes:
                print('No valid subtypes', collection_article)
                continue

            valid_members = []
            for member in members:
                article_name = WikiAPI.extract_id(member)

                article_is_valid = False
                article_types = articles_types.get(article_name, [])
                for article_type in article_types:
                    if article_type in collection_valid_subtypes[CORRECT]:
                        article_is_valid = True
                        break

                if article_is_valid:
                    valid_members.append(article_name)
                    count_valid_members += 1
                    # print('Valid', article_name, article_types, 'IN', collection_article,
                    #       list(collection_valid_subtypes[CORRECT])[:10])
                else:
                    # print('Not valid', article_name, article_types, 'IN', collection_article,
                    #       list(collection_valid_subtypes[CORRECT])[:10])
                    count_invalid_members += 1

            writer.write({
                'item': WikiAPI.extract_id(collection_item),
                'type': collection_type_id,
                'article': collection_article,
                'members': valid_members,
            })

        print('Members', count_valid_members, 'valid,', count_invalid_members, 'invalid')
