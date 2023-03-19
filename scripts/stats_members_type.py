import collections
from argparse import ArgumentParser
from wikimapper import WikiMapper
import jsonlines as jsonlines
from tqdm import tqdm

if __name__ == '__main__':
    parser = ArgumentParser(description='Calculate stats of category memebrs.')
    parser.add_argument('input', help='JSONL file with category members')
    args = parser.parse_args()

    all_members = []
    type_members = collections.defaultdict(set)

    with jsonlines.open(args.input) as reader:
        for obj in tqdm(reader):
            type = obj['type']
            members = [x[1] for x in obj['members']]
            all_members.extend(members)
            type_members[type].update(members)

        print('Articles', len(all_members))
        print('Unique articles', len(set(all_members)))
        print('unique pairs: article, category type', sum([len(v) for v in type_members.values()]))
        print('Unique types of categories', len(type_members))

        # Articles 27407336
        # Unique articles 7058361
        # unique pairs: article, category type 8947502
        # Unique types of categories 2846

        mapper = WikiMapper("data/index_enwiki-latest.db")
        articles_wikidata = 0
        for article in tqdm(set(all_members)):
            article = article.replace(' ', '_')
            wikidata_id = mapper.title_to_id(article)
            if wikidata_id is not None:
                articles_wikidata += 1
            # else:
            #     print(article)
        print('Unique articles with wikidata', articles_wikidata)
        
        # Unique articles with wikidata 7030117
