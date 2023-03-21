import csv
from argparse import ArgumentParser

import jsonlines as jsonlines
import regex
from ens_normalize import ens_force_normalize, DisallowedLabelError
from tqdm import tqdm

from download_articles_types import extract_id


def curate_name(collection_article: str):
    name = extract_id(collection_article)
    name = name.replace('_', ' ')
    name = regex.sub('^List of ', '', name)
    name = regex.sub('^Category:', '', name)
    name = name[0].upper() + name[1:]
    return name


def curate_members(members):
    curated_members = []
    for member in members:
        try:
            member = member.replace('.', '')
            curated = ens_force_normalize(member)  # TODO: add cache so one article is only once normalized
            curated_members.append(curated)
        except DisallowedLabelError as e:
            print(member, e)

    return curated_members


if __name__ == '__main__':
    parser = ArgumentParser(description='Prepare collections for ElasticSearch.')
    parser.add_argument('input', help='JSONL file with validated category/list members')
    parser.add_argument('qrank', help='CSV from https://qrank.wmcloud.org/')
    parser.add_argument('output', help='JSONL file with collections')
    args = parser.parse_args()

    ranks = {}
    with open(args.qrank, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for id, rank in tqdm(reader):
            ranks[id] = int(rank)

    with jsonlines.open(args.input) as reader, jsonlines.open(args.output, mode='w') as writer:
        for obj in tqdm(reader):
            collection_item = obj['item']
            collection_type = obj['type']
            collection_article = obj['article']
            members = obj['members']

            collection_rank = ranks.get(collection_item, 0)

            collection_name = curate_name(collection_article)
            collection_members = curate_members(members)

            if collection_members:
                writer.write({
                    'collection_name': collection_name,
                    'collection_members': collection_members,
                    'collection_description': '',
                    'collection_keywords': [],
                    'collection_image': '',
                    'metadata': {
                        'collection_wikipedia_link': collection_article,
                        # link to category or "list of" article: https://en.wikipedia.org/wiki/List_of_sovereign_states or https://en.wikipedia.org/wiki/Category:Dutch_people
                        'collection_wikidata_id': collection_item,
                        # part of Wikidata url (http://www.wikidata.org/entity/Q11750): Q11750
                        'collection_type_wikidata_id': collection_type,
                        # part of Wikidata url (http://www.wikidata.org/entity/Q3624078): Q3624078
                        'collection_articles': members,
                        'collection_rank': collection_rank,
                        # score based on popularity of category or "list of" article
                    },

                    'type': 'template',
                    'curated': False,  # manually curated by NameHash
                    'version': 0,
                    'owner': '',
                    'public': True,  # public or private collection
                    'category': '',  # category of collection
                    'datetime': '',
                })
