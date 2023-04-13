from argparse import ArgumentParser

import jsonlines as jsonlines

from tqdm import tqdm

from functions import WikiAPI
import multiprocessing

if __name__ == '__main__':
    parser = ArgumentParser(description='Cache force_normalize')
    parser.add_argument('input', help='JSONL file with ')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')

    args = parser.parse_args()

    wiki_api = WikiAPI()

    pool = multiprocessing.Pool(100)

    with jsonlines.open(args.input) as reader:
        for obj in tqdm(reader, total=args.n):
            members = obj['members']

            collection_members = wiki_api.curate_members(members)

            pool.map(wiki_api.get_interesting_score, [member.curated for member in collection_members])

            # for member in tqdm(collection_members):
            #     interesting_score, best_tokenization = wiki_api.get_interesting_score(member.curated)
            # print(interesting_score, best_tokenization)
            # break
            # break
