import re
from argparse import ArgumentParser

import jsonlines as jsonlines
import rocksdict
from rocksdict import AccessType

from tqdm import tqdm

from prepare_members_names import Collection, uniq_members


def merge_collections(collection1: Collection, collection2: Collection) -> Collection:
    collection1.members.extend(collection2.members)
    collection1.members = sorted(collection1.members, key=lambda x: x.rank, reverse=True)
    collection1.members = list(uniq_members(collection1.members))

    collection1.valid_members_count += collection2.valid_members_count
    collection1.invalid_members_count += collection2.invalid_members_count

    collection1.keywords.extend(collection2.keywords)
    collection1.types = list(set(collection1.types + collection2.types))

    if not collection1.description:
        collection1.description = collection2.description

    if not collection1.image:
        collection1.image = collection2.image

    if not collection1.page_banner:
        collection1.page_banner = collection2.page_banner

    collection1.rank = max(collection1.rank, collection2.rank)

    collection1.is_merged = True

    return collection1


filter_types = {
    'Q11266439',  # Wikimedia template 17183
    'Q4663261',  # Wikipedia:Stub 14390 # TODO all have wrong type causing valid members = 0 
    'Q13406463',  # Wikimedia list article 4181
    'Q11753321',  # Wikimedia navigational template 932
    'Q20769160',  # Wikimedia userbox template 759
    'Q30432511',  # Wikimedia meta category 5
    'Q4167836',  # Wikimedia category 203
    'Q33532284',  # Wikimedia list of lists
}


def should_filter_by_type(collection):
    return set([type[0] for type in collection.types]) & filter_types


filter_name_prefixes = (
    'Wikipedia:',
)


def should_filter_by_prefix(collection):
    return collection.name.startswith(filter_name_prefixes)


def should_filter_by_by(collection):
    m = re.search(' by ([^ ]*)', collection.name)
    if not m: return False
    by_what = m.group(1)
    return by_what[0].islower()


if __name__ == '__main__':
    parser = ArgumentParser(description='')
    parser.add_argument('lists', help='JSONL file with validated category/list members')
    parser.add_argument('categories', help='JSONL file with validated category/list members')
    parser.add_argument('output', help='JSONL file with collections')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')
    args = parser.parse_args()

    db4 = rocksdict.Rdict('data/db4.rocks', access_type=AccessType.read_only())

    # for k,v in db4.items():
    #     print(f'{k} -> {v}')

    count_lists = count_categories = count_written = 0
    count_merges = count_filtered_by_type = count_merges_by_name = count_filtered_by_prefix = count_filtered_by_by = 0

    categories_related_to_list = {}
    lists = {}
    list_names = {}

    with jsonlines.open(args.output, mode='w') as writer:
        with jsonlines.open(args.lists) as reader:
            for obj in tqdm(reader, desc='Reading lists'):
                count_lists += 1
                collection = Collection.from_dict(obj)

                if should_filter_by_type(collection):
                    count_filtered_by_type += 1
                    continue

                if should_filter_by_prefix(collection):
                    count_filtered_by_prefix += 1
                    continue

                if should_filter_by_by(collection):
                    count_filtered_by_by += 1
                    continue

                lists[collection.item] = collection
                list_names[collection.name] = collection
                try:
                    same_as = db4[collection.item]['category_related_to_list']
                    # print(f'{collection.item} -> {same_as}')
                    for category_wikidata_id in same_as:
                        categories_related_to_list[category_wikidata_id] = collection
                except KeyError:
                    pass
            print(f'Categories related to list: {len(categories_related_to_list)}')

        with jsonlines.open(args.categories) as reader:
            for obj in tqdm(reader, total=args.n, desc='Reading categories'):
                count_categories += 1
                collection = Collection.from_dict(obj)

                if should_filter_by_type(collection):
                    count_filtered_by_type += 1
                    continue

                if should_filter_by_prefix(collection):
                    count_filtered_by_prefix += 1
                    continue

                if should_filter_by_by(collection):
                    count_filtered_by_by += 1
                    continue

                # TRY MERGE BY TYPE
                related_lists = []
                if collection.item in categories_related_to_list:
                    related_lists.append(categories_related_to_list[collection.item])
                try:
                    same_as = db4[collection.item]['list_related_to_category']
                    for list_wikidata_id in same_as:
                        try:
                            related_lists.append(lists[list_wikidata_id])
                        except KeyError:
                            pass
                except KeyError:
                    pass

                merged = False
                for list_collection in related_lists:
                    merge_collections(list_collection, collection)
                    count_merges += 1
                    merged = True
                    break

                if merged: continue

                # TRY MERGE BY NAME
                if collection.name in list_names:
                    merge_collections(list_names[collection.name], collection)
                    count_merges_by_name += 1
                    merged = True

                # WRITE NOT MERGED
                if not merged:
                    writer.write(collection.json())
                    count_written += 1

        for list_collection in tqdm(lists.values(), desc='Writing lists'):
            writer.write(list_collection.json())
            count_written += 1

    print(f'All collections: {count_lists + count_categories}')
    print(f'Lists: {count_lists}, Categories: {count_categories}, Written {count_written}')
    print(f'Merged by type {count_merges} categories into lists')
    print(f'Merged by name {count_merges_by_name} categories into lists')
    print(f'Filtered by type: {count_filtered_by_type}')
    print(f'Filtered by prefix: {count_filtered_by_prefix}')
    print(f'Filtered by by: {count_filtered_by_by}')

    # All collections: 570487
    # Lists: 108944, Categories: 461543, Written 511932
    # Merged by type 6996 categories into lists
    # Merged by name 6720 categories into lists
    # Filtered by type: 44096
    # Filtered by prefix: 743

    # about 7k have the same name
    #       3 "Castles in Portugal" Q11888
    #       3 "Castles in Greece"
    #       2 "Zoroastrians"
    #       2 "Zoos in Canada"
    #       2 "Zionists"
    #       2 "Zimbabwean musicians"
    #       2 "Zayyanid dynasty"
    #       2 "Zambian films"

    # TODO filter members starting with: listsof, listof
    # TODO merge by letters?
    # Acronyms: 0–9
    # Airports by ICAO code: A
    # Compositions for viola: A to B
    # Drugs: Cj–Cl
    # "Drugs: Pro–Prz"
    # "Films: numbers"
    # "G.I. Joe: A Real American Hero characters (A–C)"
    # Meanings of minor planet names: 100001–101000
    # Municipalities in the Czech Republic: A – I
    # Towns and cities with 100,000 or more inhabitants/country: C-D-E-F
    # Winter Olympics venues: 1–9 to B
