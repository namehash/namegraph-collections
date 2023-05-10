from argparse import ArgumentParser

import jsonlines as jsonlines
from more_itertools import ichunked

from tqdm import tqdm

from functions import WikiAPI
import multiprocessing

wiki_api = WikiAPI()
def do(member):
    member = wiki_api.curate_member(member)
    if member:
        wiki_api.get_interesting_score(member.curated)

if __name__ == '__main__':
    parser = ArgumentParser(description='Cache force_normalize')
    parser.add_argument('input', help='JSONL file with ')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')
    parser.add_argument('-c', default=100, type=int, help='chunk size')

    args = parser.parse_args()

    

    pool = multiprocessing.Pool(100)

    with jsonlines.open(args.input) as reader:
        for objs in ichunked(tqdm(reader, total=args.n), args.c):
            chunk_members=[]
            for obj in objs:
                members = [x[1] for x in obj['members']]
                chunk_members.extend(members)
                
            # collection_members = wiki_api.curate_members(members)


            # def curate_members(self, members: list[str]) -> list[Member]:
            #     curated_members = []
            # 
            #     for member in members:
            #         member = self.curate_member(member)
            #         if member:
            #             curated_members.append(member)
            # 
            #     return curated_members
            pool.map(do, chunk_members)

            # for member in tqdm(collection_members):
            #     interesting_score, best_tokenization = wiki_api.get_interesting_score(member.curated)
            # print(interesting_score, best_tokenization)
            # break
            # break
