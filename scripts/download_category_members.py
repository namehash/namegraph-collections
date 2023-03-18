import time
import urllib
from argparse import ArgumentParser
import json
import jsonlines

import tqdm
import wikipediaapi
from SPARQLWrapper import SPARQLWrapper, JSON



wiki_wiki = wikipediaapi.Wikipedia(
    language='en',
    extract_format=wikipediaapi.ExtractFormat.WIKI
)
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

wiki_wiki = wikipediaapi.Wikipedia(
    language='en',
    extract_format=wikipediaapi.ExtractFormat.WIKI
)


def category_members(category_name: str) -> list:
    members = set()

    category = wiki_wiki.page(category_name, unquote=True)
    for member in wiki_wiki.categorymembers(category, cmnamespace=0).values():
        if member.ns == wikipediaapi.Namespace.MAIN:
            members.add((member.pageid, member.title))

    return list(members)

def category_members2(category_name: str) -> list:
    a = """SELECT ?item ?pageid ?title WHERE {{
      SERVICE wikibase:mwapi {{
         bd:serviceParam wikibase:api "Generator" .
         bd:serviceParam wikibase:endpoint "en.wikipedia.org" .
         bd:serviceParam mwapi:gcmtitle '{category_name}' .
         bd:serviceParam mwapi:generator "categorymembers" .
         bd:serviceParam mwapi:gcmprop "ids|title|type" .
         bd:serviceParam mwapi:gcmlimit "max" .
        ?item wikibase:apiOutputItem mwapi:item .
                 ?pageid wikibase:apiOutput "@pageid" .
         ?title wikibase:apiOutput "@title" .
      }}
    }}""".format(category_name=category_name, type=type)

    sparql.setQuery(a)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    res = [item['itemLabel']['value'] for item in results['results']['bindings']]
    return res

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='JSON file with categories')
    parser.add_argument('output', help='JSONL file for the output collections')
    args = parser.parse_args()

    with open(args.input, 'r', encoding='utf-8') as f:
        lists = json.load(f)

    #TODO add continue download option

    with jsonlines.open(args.output, mode='w') as writer:
        for wikicategory in tqdm.tqdm(lists[148724:]):
            en_label = wikicategory['article'].split('/')[-1]

            try:
                # time.sleep(0.1)
                members = category_members(en_label)
                wikicategory['members'] = members
                writer.write(wikicategory)
                
            except KeyError as e:
                print(e)
                print(en_label)
            except json.decoder.JSONDecodeError as e:
                print(e)
                print(en_label)
            except urllib.error.HTTPError as e:
                print(e)
                print(en_label)
                time.sleep(5)
            except Exception as e:
                print(e)
                print(en_label)
                time.sleep(5)
