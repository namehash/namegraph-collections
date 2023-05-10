import collections
import re
from argparse import ArgumentParser

import jsonlines as jsonlines
import rocksdict
from rocksdict import AccessType

from tqdm import tqdm

from prepare_members_names import Collection, uniq_members
from merge_lists_and_categories import merge_collections

if __name__ == '__main__':
    parser = ArgumentParser(description='')
    parser.add_argument('collections', help='')
    parser.add_argument('output', help='JSONL file with collections')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')
    args = parser.parse_args()

    count_matches = count_merged = 0

    with jsonlines.open(args.output, mode='w') as writer:
        to_merge = collections.defaultdict(list)
        with jsonlines.open(args.collections) as reader:
            for obj in tqdm(reader, desc='Reading collections', total=args.n):
                name = obj['name']
                # grep -E "([,:–] [A-Z0-9]+[a-z]* ?([–-]| to ) ?[^ ]+\"$)|((:|,|–|starting with) [A-Z]\"$)" names.txt | sort | less | wc -l
                m = re.search('(.*)(([,:–(] ?[A-Z0-9]+[a-z]* ?([–-]| to ) ?[^ ]+$)|((:|,|–|starting with|\() ?[A-Z]\)?$))', name)
                if m:
                    count_matches += 1
                    prefix = m.group(1)
                    range = m.group(2)
                    to_merge[prefix].append(Collection.from_dict(obj))
                    # print(f'{name} -> {[m.group(1), m.group(2)]}')
                    # collection = Collection.from_dict(obj)
                else:
                    writer.write(obj)

        for prefix, collections in to_merge.items():
            if len(collections) > 1:
                print(f'Merging {prefix}')
                merged = collections[0]
                for collection in collections[1:]:
                    merged = merge_collections(merged, collection)
                merged.name = prefix
                writer.write(merged.json())
                count_merged += len(collections)
            else:
                writer.write(collections[0].json())

    print(f'Matches: {count_matches}')
    print(f'Merged: {count_merged}')
