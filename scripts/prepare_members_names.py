import csv
from argparse import ArgumentParser
from urllib.parse import unquote

import jsonlines as jsonlines
import regex
import rocksdict
from rocksdict import AccessType
from tqdm import tqdm

from functions import WikiAPI, memoize_ram, Member

from hydra import initialize_config_module, compose
from inspector.label_inspector import Inspector


# from cache_interesting_score_local import get_interesting_score2

def strip_eth(name: str) -> str:
    return name[:-4] if name.endswith('.eth') else name


def read_csv_domains(path: str) -> dict[str, str]:
    domains: dict[str, str] = {}
    with open(path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in tqdm(reader, desc='Reading domains'):
            assert len(row) == 3
            name, interesting_score, status = row
            name = strip_eth(name)
            domains[name] = status

    return domains


@memoize_ram
def get_interesting_score2(label):
    rjson = inspector.analyse_label(label, truncate_confusables=0, truncate_graphemes=0, pos_lemma=False)
    try:
        interesting_score = rjson['interesting_score']
        tokenizations = rjson['tokenizations']
        try:
            best_tokenization = [token['token'] for token in tokenizations[0]['tokens']]
        except:
            best_tokenization = []
        return interesting_score, best_tokenization
    except:
        return None, []


def uniq(collection_members):
    seen = set()
    for member in collection_members:
        if member.curated not in seen:
            seen.add(member.curated)
            yield member


class Collection:
    def __init__(self):
        self.item = None
        self.types = None
        self.article = None
        self.name = None
        self.members: list[Member] | None = None
        self.valid_members_count = None
        self.invalid_members_count = None
        self.keywords = []
        self.description = None
        self.image = None
        self.page_banner = None
        self.rank = None

    def json(self):
        return {
            'item': self.item,
            'types': self.types,
            'article': self.article,
            'name': self.name,
            'members': [member.json() for member in self.members],
            'valid_members_count': self.valid_members_count,
            'invalid_members_count': self.invalid_members_count,
            'keywords': self.keywords,
            'description': self.description,
            'image': self.image,
            'page_banner': self.page_banner,
            'rank': self.rank,
        }

    @classmethod
    def from_dict(cls, data):
        collection = cls()

        collection.item = data['item']
        collection.types = data['types']
        collection.article = data['article']
        collection.name = data['name']
        collection.members = [Member.from_dict(member_data) for member_data in data['members']]
        collection.valid_members_count = data['valid_members_count']
        collection.invalid_members_count = data['invalid_members_count']
        collection.keywords = data['keywords']
        collection.description = data['description']
        collection.image = data['image']
        collection.page_banner = data['page_banner']
        collection.rank = data['rank']
        return collection


if __name__ == '__main__':
    parser = ArgumentParser(description='Prepare collections for ElasticSearch.')
    parser.add_argument('input', help='JSONL file with validated category/list members')
    parser.add_argument('qrank', help='CSV from https://qrank.wmcloud.org/')
    parser.add_argument('output', help='JSONL file with collections')

    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')

    args = parser.parse_args()

    db5 = rocksdict.Rdict('data/db5.rocks', access_type=AccessType.read_only())

    wiki_api = WikiAPI()
    wiki_api.init_wikimapper()

    initialize_config_module(version_base=None, config_module='inspector_conf')
    config = compose(config_name="prod_config")
    inspector = Inspector(config)

    ranks = {}
    with open(args.qrank, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for id, rank in tqdm(reader, desc='Reading qrank'):
            ranks[id] = int(rank)

    domains = read_csv_domains('data/suggestable_domains.csv')

    # TODO add multiprocessing
    with jsonlines.open(args.input) as reader, jsonlines.open(args.output, mode='w') as writer:
        for obj in tqdm(reader, total=args.n):
            collection = Collection()
            collection.item = obj['item']
            collection.types = obj['type']
            collection.article = obj['article']
            members = obj['members']
            collection.valid_members_count = obj['valid_members_count']
            collection.invalid_members_count = obj['invalid_members_count']
            # keywords = []
            # description = None
            # image = None
            # page_banner = None
            collection.rank = ranks.get(collection.item, 0)

            collection.name = wiki_api.curate_name(collection.article)
            # print(collection_article, collection_name)
            # alwats both name and label are present, only few items have only description
            if collection.item in db5:
                info_data = db5[collection.item]
                try:
                    wikidata_label = info_data['label']
                    # wikidata_name = info_data['name']
                    collection.keywords.append(wikidata_label)
                except KeyError:
                    print('No label/name', info_data)

                try:
                    wikidata_description = info_data['description']
                    if wikidata_description not in ['Wikimedia list article', 'Wikimedia category']:
                        # print('Description', collection_article, collection_item, wikidata_description)
                        wikidata_description = regex.sub('^[Ww]ikimedia ', '', wikidata_description)
                        collection.description = wikidata_description
                except KeyError:
                    pass

                try:
                    collection.images = [unquote(url) for url in info_data['image']]
                except KeyError:
                    pass

                try:
                    collection.page_banners = [unquote(url) for url in info_data['page_banner']]
                except KeyError:
                    pass

            else:
                print('No item in db5', collection.item)
                # continue

            # add keywords
            collection.keywords = wiki_api.mapper.id_to_titles(collection.item)
            collection.keywords = [wiki_api.curate_name(k) for k in collection.keywords]
            collection.keywords = [k for k in collection.keywords if k != collection.name]
            # keywords 

            # if wikidata_label != wikidata_name:  # always the same
            #     print(collection_article, collection_name, wikidata_label, wikidata_name)

            # wikidata_label is not updated, but does not have parenthesis
            # we can add it as keyword

            # wikidata_label = regex.sub('^list of ', '', wikidata_label)
            # wikidata_label = regex.sub('^Category:', '', wikidata_label)
            # wikidata_label = wikidata_label[0].upper() + wikidata_label[1:]

            # if collection_name != wikidata_label:           
            #     print(collection_article)
            #     print(collection_name)
            #     print(wikidata_label)
            #     print()
            # decode
            collection_members = []
            for member_id, member_name in members:
                member = wiki_api.curate_member(member_name)

                if member is None:
                    if member_id in db5:
                        info_data = db5[member_id]
                        try:
                            wikidata_label = info_data['label']
                            member = wiki_api.curate_member(wikidata_label)
                        except KeyError:
                            print('No label/name', info_data)

                if member:
                    collection_members.append(member)

                    member.interesting_score = get_interesting_score2(member.curated)[0]
                    # add qrank
                    member.rank = ranks.get(member_id, 0)
                    # add statuses
                    member.status = domains.get(member.curated, None)

            # obj['keywords'] = keywords
            # obj['description'] = description
            # obj['image'] = image
            # obj['page_banner'] = page_banner
            # obj['collection_name'] = collection_name
            collection_members = uniq(sorted(collection_members, key=lambda m: m.rank, reverse=True))

            writer.write(collection.json())

            # if member is None and curated_wikidata_label is not None:
            #     print(member_name, curated_wikidata_label.curated)
            #     print()
            # SS schutzstaffel
            # Zo (people) zopeople
            # F2 (film) f2-funandfrustration
            # JJ (video game) the3-dbattlesofworldrunner
            # F/A fighterattacker
            # Đỗ Mậu domau
            # MJ (Marvel Cinematic Universe) michellejones

            # maybe check redirects?

            # if member:
            #     curated_members.append(member)
            # collection_members = wiki_api.curate_members(members)

# TODO calc interesting score
# TODO add qrank
# TODO add statuses
# TODO add keywords, images, descriptions?
# lists 4h
