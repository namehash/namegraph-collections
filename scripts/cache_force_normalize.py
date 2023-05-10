from argparse import ArgumentParser

import jsonlines as jsonlines

from tqdm import tqdm

from functions import WikiAPI

if __name__ == '__main__':
    parser = ArgumentParser(description='Cache force_normalize')
    parser.add_argument('input', help='JSONL file with ')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')

    args = parser.parse_args()

    wiki_api = WikiAPI()

    unique_members = set()

    with jsonlines.open(args.input) as reader:
        for obj in tqdm(reader, total=args.n):
            members = [x[1] for x in obj['members']]
            collection_members = wiki_api.curate_members(members)
            unique_members.update([member.curated for member in collection_members])

    print(len(unique_members))  # 2142399
