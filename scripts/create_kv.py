import bz2
import sys

import lightrdf
from tqdm import tqdm
import shelve

parser = lightrdf.Parser()

predicates_one = {
    '<http://schema.org/about>',
    # '<http://www.wikidata.org/prop/direct/P31>',  # instance of - wiele
    # '<http://www.wikidata.org/prop/direct/P279>',  # subclass of - wiele
    # '<http://www.wikidata.org/prop/direct/P360>',  # is a list of -  wiele https://www.wikidata.org/wiki/Q177410
    # '<http://www.wikidata.org/prop/direct/P4224>', # category contains - wiele https://www.wikidata.org/wiki/Q7235423
    # '<http://www.wikidata.org/prop/direct/P1753>',  # list related to category - wiele
    # '<http://www.wikidata.org/prop/direct/P1754>',  # category related to list - wiele
    # '<http://www.wikidata.org/prop/direct/P18>',  # image - wiele
    # '<http://www.wikidata.org/prop/direct/P948>',  # page banner - wiele
    '<http://schema.org/name>',
    '<http://www.w3.org/2000/01/rdf-schema#label>',
    '<http://schema.org/description>'
}

dbs = {
    'db1': {'about'},
    'db2': {'instance_of', 'subclass_of'},
    'db3': {'is_a_list_of', 'category_contains'}, #TODO add name
    'db4': {'list_related_to_category', 'category_related_to_list'},
    'db5': {'name', 'label', 'description', 'image', 'page_banner'},
}

mapping = {
    '<http://schema.org/about>': 'about',
    '<http://www.wikidata.org/prop/direct/P31>': 'instance_of',  # instance of
    '<http://www.wikidata.org/prop/direct/P279>': 'subclass_of',  # subclass of
    '<http://www.wikidata.org/prop/direct/P360>': 'is_a_list_of',  # is a list of
    '<http://www.wikidata.org/prop/direct/P4224>': 'category_contains',  # category contains
    '<http://www.wikidata.org/prop/direct/P1753>': 'list_related_to_category',  # list related to category
    '<http://www.wikidata.org/prop/direct/P1754>': 'category_related_to_list',  # category related to list
    '<http://www.wikidata.org/prop/direct/P18>': 'image',  # image
    '<http://www.wikidata.org/prop/direct/P948>': 'page_banner',  # page banner
    '<http://schema.org/name>': 'name',
    '<http://www.w3.org/2000/01/rdf-schema#label>': 'label',
    '<http://schema.org/description>': 'description',
}
predicates_one = {'about', 'name', 'label', 'description'}

filter_instances = {
    '<http://www.wikidata.org/entity/Q13442814>',  # scholarly article
    '<http://www.wikidata.org/entity/Q7318358>',  # review article
    '<http://www.wikidata.org/entity/Q4167410>',  # Wikimedia disambiguation page
    '<http://www.wikidata.org/entity/Q11266439>',  # Wikimedia template
}


# Wikimedia internal item (Q17442446) - nie bo są tam tez normalne kategorie, np. filmography

def clean(so):
    """Clean subject or object"""
    prefixes = [
        '<http://www.wikidata.org/entity/',
        '<https://en.wikipedia.org/wiki/',
        '<http://commons.wikimedia.org/wiki/',
    ]
    for prefix in prefixes:
        if so.startswith(prefix):
            so = so[len(prefix):-1]
            return so
    # warn
    if so.startswith('"') and so.endswith('"@en'):
        so = so[1:-4]
        return so
    print(f'Not cleaned: {so}', file=sys.stderr)
    raise ValueError
    # return so


def entity_generator(path):
    entity = {}
    last_subject = None
    with bz2.open(path, "rb") as f:
        for triple in tqdm(parser.parse(f, format='nt'), total=392602664):
            subject, predicate, object = triple
    
            try:
                predicate = mapping[predicate]
            except KeyError:
                continue
    
            if predicate == 'instance_of' and object in filter_instances:
                continue
    
            try:
                subject = clean(subject)
                object = clean(object)
            except ValueError:
                continue
    
            if last_subject is None:
                last_subject = subject
    
            if subject != last_subject:
                yield last_subject, entity
                entity = {}
    
            if predicate in predicates_one:
                entity[predicate] = object
            else:
                if predicate not in entity:
                    entity[predicate] = []
                entity[predicate].append(object)
    
            last_subject = subject
    
        if entity:
            yield last_subject, entity


def split_dict(entity, mappings):
    result = {}
    for db_name, predicates in mappings.items():
        result[db_name] = {}
        for predicate in predicates:
            if predicate in entity:
                result[db_name][predicate] = entity[predicate]
    return result


# d = shelve.open('filtered.shelve')
# import vedis
# d = vedis.Vedis('filtered.vedis')
import rocksdict

# d = rocksdict.Rdict('filtered.rocksdict')

rockdbs = {}
for db_name, predicates in dbs.items():
    rockdbs[db_name] = rocksdict.Rdict('data/' + db_name + '.rocks')

for subject, entity in entity_generator(sys.argv[1]):
    # d[subject] = entity

    splitted_entity = split_dict(entity, dbs)

    for db_name, entity in splitted_entity.items():
        if entity:
            rockdbs[db_name][subject] = entity

    # print(subject,entity)
    # for predicate, objects in entity.items():
    #     if predicate in predicates_one:
    #         if len(objects) > 1:
    #             print(subject, predicate, objects)
# d.close()

for db in rockdbs.values():
    db.close()
# kilka baz osobnych?
