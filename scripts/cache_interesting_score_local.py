from argparse import ArgumentParser

import jsonlines as jsonlines

from tqdm import tqdm

from functions import WikiAPI, memoize_ram
import multiprocessing

from hydra import initialize_config_module, compose
from inspector.label_inspector import Inspector


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


if __name__ == '__main__':
    parser = ArgumentParser(description='Cache force_normalize')
    parser.add_argument('input', help='JSONL file with ')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')

    args = parser.parse_args()

    wiki_api = WikiAPI()

    initialize_config_module(version_base=None, config_module='inspector_conf')
    config = compose(config_name="prod_config")
    inspector = Inspector(config)

    
    unique_members = set()

    with jsonlines.open(args.input) as reader:
        for obj in tqdm(reader, total=args.n):
            members = obj['members']

            collection_members = wiki_api.curate_members(members)
            unique_members.update([member.curated for member in collection_members])

    print(len(unique_members))

    with multiprocessing.Pool(8) as p:
        r = list(tqdm(p.imap(get_interesting_score2, unique_members), total=len(unique_members)))