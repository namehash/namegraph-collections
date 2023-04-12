import json
from argparse import ArgumentParser

import rocksdict
from rocksdict import AccessType
from tqdm import tqdm

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('output', help='JSON file for the output collections')
    parser.add_argument('--mode', default='category', choices=['category', 'list'], help='mode')
    args = parser.parse_args()

    db3 = rocksdict.Rdict('data/db3.rocks', access_type=AccessType.read_only())
    db1 = rocksdict.Rdict('data/db1_rev.rocks', access_type=AccessType.read_only())

    # for wikidata_id, predicates in db1.items():
    #     print(wikidata_id, predicates)

    if args.mode == 'category':
        predicate = 'category_contains'
    elif args.mode == 'list':
        predicate = 'is_a_list_of'

    # there might more than one type of list/category

    articles = []
    for wikidata_id, predicates in tqdm(db3.items()):
        try:
            # print(wikidata_id, predicates)
            # print(db1[wikidata_id])
            if predicate in predicates:
                article_name = db1[wikidata_id]
                
                if args.mode == 'category':
                    if not article_name.startswith('Category:'):
                        continue
                elif args.mode == 'list':
                    if article_name.startswith('Lists_of:'):
                        continue
                
                articles.append({
                    "item": wikidata_id,
                    "type": predicates[predicate],
                    "article": article_name,
                    # "count": "221"
                })
        except KeyError:
            pass
    json.dump(articles, open(args.output, 'w', encoding='utf-8'), indent=2, ensure_ascii=False)
