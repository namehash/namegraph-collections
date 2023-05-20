import math
from argparse import ArgumentParser

import jsonlines as jsonlines
import numpy as np
from tqdm import tqdm

from prepare_members_names import Collection

MIN_VALUE = 1e-8

if __name__ == '__main__':
    parser = ArgumentParser(description='Prepare collections for ElasticSearch.')
    parser.add_argument('input', help='JSONL file with validated category/list members')
    parser.add_argument('output', help='JSONL file with collections')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')

    args = parser.parse_args()

    with jsonlines.open(args.input) as reader, jsonlines.open(args.output, mode='w') as writer:
        for obj in tqdm(reader, total=args.n):
            collection = Collection.from_dict(obj)

            collection.rank = max(collection.rank, 1)  # rank_feature must be positive

            status_counts = {'available': 0, 'taken': 0, 'on_sale': 0, 'recently_released': 0, 'never_registered': 0}
            if collection.members:
                for member in collection.members:
                    status = member.status
                    if status is None:
                        status = 'never_registered'
                    status_counts[status] += 1
                nonavailable_members = len(
                    [True for m in collection.members if m.status in ('taken', 'on_sale', 'recently_released')])

                # sort collection members
                collection.members = sorted(collection.members,
                                            key=lambda x: math.log(x.rank + 1, 2) / len(x.curated),
                                            reverse=True)

                writer.write({
                    'data': {  # owner controlled
                        'collection_name': collection.name,
                        'names': [{
                            'normalized_name': member.curated,
                            'avatar_override': '',
                            'tokenized_name': member.tokenized,
                        } for member in collection.members],  # TODO sort
                        'collection_description': collection.description,
                        'collection_keywords': collection.keywords,
                        'collection_image': collection.image[0] if collection.image else None,  # TODO
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
                        # a pointer to another collection. This field could be set whenever we create a collection from a template (reference back to origin template) or it could be set whenever a user 'duplicates' another user generated collection.
                        'members_count': len(collection.members),
                    },
                    'template': {  # template generator controlled
                        'collection_wikipedia_link': collection.article,
                        # link to category or "list of" article: https://en.wikipedia.org/wiki/List_of_sovereign_states or https://en.wikipedia.org/wiki/Category:Dutch_people
                        'collection_wikidata_id': collection.item,
                        # part of Wikidata url (http://www.wikidata.org/entity/Q11750): Q11750
                        'collection_types': collection.types,
                        # part of Wikidata url (http://www.wikidata.org/entity/Q3624078): Q3624078
                        # 'collection_articles': members,
                        'collection_rank': collection.rank,
                        # score based on popularity of category or "list of" article
                        'translations_count': None,  # TODO
                        'has_related': None,  # has related category/list # TODO

                        'collection_images': collection.image,
                        'collection_page_banners': collection.page_banner,

                        'names': [{
                            'normalized_name': member.curated,
                            # 'tokenized_name': member.tokenized,
                            'system_interesting_score': member.interesting_score,
                            'rank': member.rank,
                            'cached_status': member.status,
                            # 'translations_count': None,
                        } for member in collection.members],  # TODO sort

                        # below metrics calculated on members
                        'members_rank_mean': max(np.mean([m.rank for m in collection.members]), MIN_VALUE),
                        'members_rank_median': max(np.median([m.rank for m in collection.members]), MIN_VALUE),
                        'members_system_interesting_score_mean': max(
                            np.mean([m.interesting_score for m in collection.members]), MIN_VALUE),
                        'members_system_interesting_score_median': max(
                            np.median([m.interesting_score for m in collection.members]), MIN_VALUE),
                        'valid_members_count': collection.valid_members_count,
                        'invalid_members_count': collection.invalid_members_count,
                        'valid_members_ratio': collection.valid_members_count / (
                                collection.valid_members_count + collection.invalid_members_count) if collection.valid_members_count + collection.invalid_members_count > 0 else 0.0,
                        'nonavailable_members_count': nonavailable_members,
                        'nonavailable_members_ratio': max(nonavailable_members / len(collection.members), MIN_VALUE),

                        'is_merged': collection.is_merged,
                        'available_count': status_counts['available'],
                        'taken_count': status_counts['taken'],
                        'on_sale_count': status_counts['on_sale'],
                        'recently_released_count': status_counts['recently_released'],
                        'never_registered_count': status_counts['never_registered'],
                    },
                    'name_generator': {  # Lambda NameGenerator preprocessor controlled

                    },
                })
