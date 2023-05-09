import collections

from argparse import ArgumentParser

import jsonlines as jsonlines

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


if __name__ == '__main__':
    parser = ArgumentParser(description='Filter collections by name duplicates')
    parser.add_argument('collections', help='JSONL file ')
    parser.add_argument('output', help='JSONL file with collections')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')
    args = parser.parse_args()

    count_merged = 0
    with jsonlines.open(args.output, mode='w') as writer:
        collection_names_count = collections.defaultdict(int)
        with jsonlines.open(args.collections) as reader:
            for obj in tqdm(reader, desc='Reading collections', total=args.n):
                name = obj['name']
                collection_names_count[name] += 1

        collection_names = collections.defaultdict(list)
        with jsonlines.open(args.collections) as reader:
            for obj in tqdm(reader, desc='Reading collections', total=args.n):
                name = obj['name']
                if collection_names_count[name] > 1:
                    collection_names[name].append(Collection.from_dict(obj))
                else:
                    writer.write(obj)

        for collections in collection_names.values():
            merged = collections[0]
            for collection in collections[1:]:
                merged = merge_collections(merged, collection)
            writer.write(merged.json())
            count_merged += len(collections)

    print(f'Merged: {count_merged}')
