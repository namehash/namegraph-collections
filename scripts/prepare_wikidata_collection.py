from argparse import ArgumentParser

import jsonlines as jsonlines

from tqdm import tqdm

from scripts.functions import WikiAPI

if __name__ == '__main__':
    parser = ArgumentParser(description='Prepare collections for ElasticSearch.')
    parser.add_argument('input', help='JSONL file with ')
    parser.add_argument('output', help='JSONL file with collections')
    parser.add_argument('-w', '--wiki_mapper', default='data/index_enwiki-latest.db',
                        help='path to WikiMapper database')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')

    args = parser.parse_args()

    wiki_api = WikiAPI()
    wiki_api.init_wikimapper(args.wiki_mapper)

    # TODO add sorting first because jq can't do this
    with jsonlines.open(args.input) as reader, jsonlines.open(args.output, mode='w') as writer:
        for obj in tqdm(reader, total=args.n):
            collection_item = obj['id']
            collection_rank = obj['rank']
            # collection_type = obj['type']
            # collection_article = obj['article']
            members = [x['label'] for x in obj['instances']]
            if not members:
                continue
            # print(collection_item, members)
            try:
                collection_name = wiki_api.mapper.id_to_titles(collection_item)[0]  # TODO get label from wikidata
            except IndexError:
                continue
            collection_members = wiki_api.curate_members(members)
            collection_keywords = wiki_api.mapper.id_to_titles(collection_item)[1:]
            # print(wiki_api.mapper.id_to_titles(collection_item))
            # print(collection_members)
            # print()

            # continue
            if collection_members:
                writer.write({
                    'data': {  # owner controlled
                        'collection_name': collection_name,
                        'names': [{
                            'normalized_name': curated,
                            'avatar_override': '',
                            'tokenized_name': tokenized,
                        } for curated, tokenized in collection_members],
                        'collection_description': '',
                        'collection_keywords': collection_keywords,
                        'collection_image': '',
                        'public': True,  # public or private collection

                        'archived': False,
                        # By default false. This would be used exclusively for the narrowly defined purpose of deciding if we include a collection on a user's "Collections" tab in their "Account Page".
                    },
                    'curation': {  # admin controlled
                        'curated': False,  # manually curated by NameHash
                        'category': '',  # Each collection can optionally be curated into 0 or 1 predefined categories.
                        'trending': False,
                        # This is a boolean, false by default, that we might use to say a collection is trending that would give it more visibility on NameHash.
                        'community-choice': False,
                        # This is a boolean, false by default, that we might use to say a collection is trending that would give it more visibility on NameHash.

                    },
                    'metadata': {  # system controlled
                        'id': '',  # UUID
                        'type': 'template',
                        'version': 0,
                        'owner': '',
                        'created': '',
                        'modified': '',
                        'votes': [],  # This could be some array of all the accounts that "upvoted" the collection.
                        'duplicated-from': '',
                        'members_count-from': len(collection_members),
                        # a pointer to another collection. This field could be set whenever we create a collection from a template (reference back to origin template) or it could be set whenever a user 'duplicates' another user generated collection.
                    },
                    'template': {  # template generator controlled
                        'collection_wikipedia_link': '',
                        # link to category or "list of" article: https://en.wikipedia.org/wiki/List_of_sovereign_states or https://en.wikipedia.org/wiki/Category:Dutch_people
                        'collection_wikidata_id': collection_item,
                        # part of Wikidata url (http://www.wikidata.org/entity/Q11750): Q11750
                        'collection_type_wikidata_id': collection_item,
                        # part of Wikidata url (http://www.wikidata.org/entity/Q3624078): Q3624078
                        'collection_articles': members,
                        'collection_rank': collection_rank,
                        # score based on popularity of category or "list of" article
                    },
                    'name_generator': {  # Lambda NameGenerator preprocessor controlled

                    },
                })
