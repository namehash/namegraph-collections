import sys
from argparse import ArgumentParser
from functools import lru_cache

import jsonlines as jsonlines
import rocksdict
from rocksdict import AccessType, Rdict
from tqdm import tqdm

from functions import WikiAPI, memoize_ram
from types_to_validate import load_articles_types

CORRECT = 'correct'
INCORRECT = 'incorrect'

NO_PARENT = 0


@lru_cache(maxsize=None)
def has_path_rocksdb_subclass(source: str, target: str) -> bool:
    global NO_PARENT
    visited = set()
    stack: list[str] = [source]
    visited.add(source)

    while stack:
        curr = stack.pop()
        if curr == target:
            return True
        try:
            neighbours = rdict[curr].get('subclass_of', [])
        except KeyError:
            try:
                neighbours = rdict[db6[curr]['same_as'][0]].get('subclass_of', [])  # try redirect
            except KeyError:
                NO_PARENT += 1
                if curr in db6 and db6[curr]["same_as"][0] in db6:
                    print(f'redirect KeyError: {curr} {db6[curr]["same_as"][0]}', file=sys.stderr)
                continue
        for neigh in neighbours:
            if neigh in visited:
                continue

            if neigh == target:
                return True

            visited.add(neigh)
            stack.append(neigh)

    return False


def has_path_rocksdb(rdict: Rdict, source: str, target: str) -> bool:
    global NO_PARENT
    entries = []
    try:
        entries += rdict[source].get('instance_of', [])
        entries += rdict[source].get('subclass_of', [])
    except KeyError:
        # print(f'rdict KeyError: {source}', file=sys.stderr)
        try:
            # print(db6[source])
            entries += rdict[db6[source]['same_as'][0]].get('instance_of', [])
            entries += rdict[db6[source]['same_as'][0]].get('subclass_of', [])
        except KeyError:
            NO_PARENT += 1
        pass
        # print(f'instance_of KeyError: {source}', file=sys.stderr)
    # return has_path_rocksdb_subclass(tuple(entries), target)
    # print(source, entries, target)
    return any([has_path_rocksdb_subclass(entry, target) for entry in entries])


if __name__ == '__main__':
    parser = ArgumentParser(description='Filter members using types')
    parser.add_argument('input', help='JSONL file with category/list members')
    # parser.add_argument('article_types', help='JSONL file with types of articles')
    # parser.add_argument('validated_types', help='JSONL file types validated with subclass of')
    parser.add_argument('output', help='JSONL file with validated category/list members')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')
    args = parser.parse_args()

    # articles_types = load_articles_types(args.article_types)
    # validated_types = load_validated_types(args.validated_types)

    rdict = rocksdict.Rdict('data/db2.rocks', access_type=AccessType.read_only())
    # db1 = rocksdict.Rdict('data/db1_rev.rocks', access_type=AccessType.read_only())
    db6 = rocksdict.Rdict('data/db6.rocks', access_type=AccessType.read_only())

    wikiapi = WikiAPI()
    wikiapi.init_wikimapper()

    count_valid_members = 0
    count_invalid_members = 0

    with jsonlines.open(args.input) as reader, jsonlines.open(args.output, mode='w') as writer:
        for obj in tqdm(reader, total=args.n):
            collection_item = obj['item']
            collection_types = obj['type']
            collection_type_ids = WikiAPI._extract_ids(collection_types)
            collection_article = WikiAPI.extract_article_name(obj['article'])
            members = obj['members']

            # collection_valid_subtypes = validated_types.get(collection_type_id, [])
            # if not collection_valid_subtypes:
            #     print('No valid subtypes', collection_article)
            #     continue

            valid_members = []
            for member in members:
                article_name = WikiAPI.extract_article_name(member)

                # article_types = articles_types.get(article_name, [])
                article_wikidata_id = wikiapi.mapper.title_to_id(member.replace(' ', '_'))
                if article_wikidata_id is None:
                    # print('No wikidata id', article_name)
                    continue

                article_is_valid = False
                # print('-', article_wikidata_id, collection_type_ids)
                for collection_type_id in collection_type_ids:
                    # print('-', article_wikidata_id, collection_type_id)
                    if has_path_rocksdb(rdict, article_wikidata_id, collection_type_id):
                        article_is_valid = True
                        break

                if article_is_valid:
                    valid_members.append((article_wikidata_id, article_name))
                    count_valid_members += 1
                    # print('Valid', article_name, article_wikidata_id, 'IN', collection_article, collection_type_id)
                else:
                    # print('Not valid', article_name, article_wikidata_id, 'IN', collection_article, collection_type_id)
                    count_invalid_members += 1

            writer.write({
                'item': WikiAPI.extract_id(collection_item),
                'type': collection_type_ids,
                'article': collection_article,
                'members': valid_members,
                'valid_members_count': len(valid_members),
                'invalid_members_count': len(members) - len(valid_members)
            })

        print('Members', count_valid_members, 'valid,', count_invalid_members, 'invalid')

    print('No parent', NO_PARENT)
