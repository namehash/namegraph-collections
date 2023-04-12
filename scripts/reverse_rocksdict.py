from argparse import ArgumentParser

import rocksdict
from rocksdict import AccessType
from tqdm import tqdm

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='')
    parser.add_argument('output', help='')
    args = parser.parse_args()

    db_input = rocksdict.Rdict(args.input, access_type=AccessType.read_only())
    db_output = rocksdict.Rdict(args.output)
    
    for wikidata_id, predicates in tqdm(db_input.items()):
        # print(wikidata_id, predicates)
        # assert predicates['about']
        db_output[predicates['about']] = wikidata_id

    db_output.close()