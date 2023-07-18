import csv
import math
import random
import sys
import time
from argparse import ArgumentParser

import jsonlines as jsonlines
import numpy as np
from ens.constants import EMPTY_SHA3_BYTES
from ens.utils import Web3
from hexbytes import HexBytes
from tqdm import tqdm

from hydra import initialize_config_module, compose
from inspector.label_inspector import Inspector

from prepare_members_names import Collection
from functions import memoize_ram

MIN_VALUE = 1e-8


def label_to_hash(label: str) -> HexBytes:
    if "." in label:
        raise ValueError(f"Cannot generate hash for label {label!r} with a '.'")
    return Web3().keccak(text=label)


@memoize_ram
def normal_name_to_hash(name: str) -> str:
    node = EMPTY_SHA3_BYTES
    if name:
        labels = name.split(".")
        for label in reversed(labels):
            labelhash = label_to_hash(label)
            assert isinstance(labelhash, bytes)
            assert isinstance(node, bytes)
            node = Web3().keccak(node + labelhash)
    return node.hex()


class AvatarEmoji:
    def __init__(self, path):
        self.load_emojis(path)

    def load_emojis(self, path):
        self.emojis = {}
        self.emoji_counts = {}
        
        with open(path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                type_id, type_name, category, count, *type_emojis = row
                if type_name == 'OTHER':
                    self.other_emojis = type_emojis
                    continue
                count = int(count)
                self.emojis[type_name] = type_emojis
                self.emoji_counts[type_name] = count

    def get_emoji(self, collection_id, types):
        #sort types by emoji count from the less popular
        types = sorted(types, key=lambda x: self.emoji_counts.get(x, sys.maxsize))
        random.seed(collection_id)
        if types and types[0] in self.emojis:
            return random.choice(self.emojis[types[0]])
        else:
            return random.choice(self.other_emojis)


if __name__ == '__main__':
    parser = ArgumentParser(description='Prepare collections for ElasticSearch.')
    parser.add_argument('input', help='JSONL file with validated category/list members')
    parser.add_argument('output', help='JSONL file with collections')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')

    args = parser.parse_args()

    initialize_config_module(version_base=None, config_module='inspector_conf')
    config = compose(config_name="prod_config")
    inspector = Inspector(config)

    current_time = time.time() * 1000

    avatar_emoji = AvatarEmoji('data/avatars-emojis.csv')

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
                                            key=lambda x: math.log(x.rank + 1, 2) / max(len(x.curated), 10),
                                            reverse=True)

                template_names = [{
                    'normalized_name': member.curated,
                    # 'tokenized_name': member.tokenized,
                    'system_interesting_score': member.interesting_score,
                    'rank': member.rank,
                    'cached_status': member.status,
                    'namehash': normal_name_to_hash(member.curated + '.eth'),
                    # 'translations_count': None,
                } for member in collection.members]

                random.seed(collection.item)
                banner_image_number = random.randint(0, 19)

                writer.write({
                    'data': {  # owner controlled
                        'collection_name': collection.name,
                        'names': [{
                            'normalized_name': member.curated,
                            'avatar_override': '',
                            'tokenized_name': member.tokenized,
                        } for member in collection.members],  # TODO sort
                        # 'collection_description': collection.description,
                        'collection_description': 'A collection of names auto-generated from Wikipedia and Wikidata using AI',
                        'collection_keywords': collection.keywords,
                        'collection_image': collection.image[0] if collection.image else None,  # TODO
                        'public': True,  # public or private collection

                        'banner_image': f'tc-{banner_image_number:02d}.png',
                        'avatar_image': None,
                        'avatar_emoji': avatar_emoji.get_emoji(collection.item, [type for cid, type in collection.types]),

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
                        'id': collection.item,  # UUID
                        'type': 'template',
                        'version': 0,
                        'owner': '0xcb8f5f88e997527d76401ce3df8c8542b676e149',
                        'created': current_time,
                        'modified': current_time,
                        'votes': [],  # This could be some array of all the accounts that "upvoted" the collection.
                        'duplicated-from': '',
                        # a pointer to another collection. This field could be set whenever we create a collection from a template (reference back to origin template) or it could be set whenever a user 'duplicates' another user generated collection.
                        'members_count': len(collection.members),
                        'collection_name_log_probability': inspector.ngrams.sequence_log_probability(
                            collection.name.lower().split(' ')),
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

                        'names': template_names,
                        'top10_names': template_names[:10],

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
