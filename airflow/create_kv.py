import bz2
import sqlite3, re
import sys
from datetime import datetime, timedelta
from textwrap import dedent

import lightrdf
from tqdm import tqdm
import rocksdict
from rocksdict import AccessType

from airflow import DAG, Dataset
from make_dag import CONFIG, WIKIDATA_FILTERED, WIKIMAPPER

from airflow.operators.python import PythonOperator

parser = lightrdf.Parser()

dbs = {
    'db1': {'about'},
    'db2': {'instance_of', 'subclass_of'},
    'db3': {'is_a_list_of', 'category_contains'},  # TODO add name
    'db4': {'list_related_to_category', 'category_related_to_list'},
    'db5': {'name', 'label', 'description', 'image', 'page_banner'},
    'db6': {'same_as'},
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
    '<http://www.w3.org/2002/07/owl#sameAs>': 'same_as',
}
predicates_one = {'about', 'name', 'label', 'description', 'same_as'}

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
        for triple in tqdm(parser.parse(f, format='nt'), total=396603875):
            subject, predicate, object = triple

            try:
                predicate = mapping[predicate]
            except KeyError:
                continue

            if predicate == 'instance_of' and object in filter_instances:
                continue

            if predicate == 'name' and subject.startswith('<https://en.wikipedia.org/wiki/'):
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

ROCKS_DB_2 = Dataset(f"{CONFIG.remote_prefix}db2.rocks")
ROCKS_DB_3 = Dataset(f"{CONFIG.remote_prefix}db3.rocks")
ROCKS_DB_4 = Dataset(f"{CONFIG.remote_prefix}db4.rocks")
ROCKS_DB_5 = Dataset(f"{CONFIG.remote_prefix}db5.rocks")
ROCKS_DB_6 = Dataset(f"{CONFIG.remote_prefix}db6.rocks")


def create_rocksdb(dbs, entity_path, db_path_prefix):
    rockdbs = {}
    for db_name, predicates in dbs.items():
        rockdbs[db_name] = rocksdict.Rdict(db_path_prefix + db_name + '.rocks')

    for subject, entity in entity_generator(entity_path):

        splitted_entity = split_dict(entity, dbs)

        for db_name, entity in splitted_entity.items():

            if entity:
                rockdbs[db_name][subject] = entity

    for db in rockdbs.values():
        db.close()


def create_reverse_rocksdb(input_path, output_path):
    db_input = rocksdict.Rdict(input_path, access_type=AccessType.read_only())
    db_output = rocksdict.Rdict(output_path)
    
    for wikidata_id, predicates in tqdm(db_input.items()):
        db_output[predicates['about']] = wikidata_id

    db_output.close()

def load_wikidata_wikipedia_mapping(input_path, db1_path, db1_rev_path):
    connection = sqlite3.connect(input_path)
    cursor = connection.cursor()

    rocksdb_direct = rocksdict.Rdict(db1_path)
    rocksdb_rev = rocksdict.Rdict(db1_rev_path)

    count = list(cursor.execute("SELECT count(*) from mapping WHERE primary_mapping = 1 AND redirect = 0"))[0][0]
    for item in tqdm(cursor.execute("SELECT wikipedia_title, wikipedia_id, wikidata_id FROM mapping WHERE primary_mapping = 1 AND redirect = 0"), total=count):
        name = re.sub(r"_", " ", item[0])
        wikidata_id = item[2]
        rocksdb_rev[wikidata_id] = name
        rocksdb_direct[name] = {"about": wikidata_id }

    rocksdb_direct.close()
    rocksdb_rev.close()

with DAG(
    "rocksdb-main",
    default_args={
        "depends_on_past": False,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "cwd": CONFIG.local_prefix,
    },
    description="Tasks related to the creation of fast KV stores",
    schedule=[WIKIDATA_FILTERED],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["db", "collection-templates"],
) as dag:
    create_rocksdb = PythonOperator(
        task_id='create-rocksdb',
        python_callable=create_rocksdb,
        op_kwargs={
            "dbs": dbs, 
            "entity_path": f"{CONFIG.local_prefix}/latest-truthy.filtered.nt.bz2",  
            "db_path_prefix": CONFIG.local_prefix
        },
        outlets=[ROCKS_DB_2, ROCKS_DB_3, ROCKS_DB_4, ROCKS_DB_5, ROCKS_DB_6,]
        #start_date=datetime(3021, 1, 1),
    )
    create_rocksdb.doc_md = dedent(
        """\
    #### Task Documentation
    The task creates a number of rocksdb databases, to store mappings between
    wikidata entitites and their properties.

    These databases do not include the mapping between Wikidata and English Wikipedia.
    """
    )

ROCKS_DB_1 = Dataset(f"{CONFIG.remote_prefix}db1.rocks")
ROCKS_DB_1_REVERSE = Dataset(f"{CONFIG.remote_prefix}db1_rev.rocks")

with DAG(
    "rocksdb-entities",
    default_args={
        "depends_on_past": False,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "cwd": CONFIG.local_prefix,
    },
    description="Tasks related to the creation of fast KV stores (Wikidata - Wikipedia)",
    schedule=[WIKIMAPPER],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["db", "collection-templates"],
) as dag:
    create_reverse = PythonOperator(
        task_id='create-rocksdb-entities',
        python_callable=load_wikidata_wikipedia_mapping,
        op_kwargs={
            "input_path": f"{CONFIG.local_prefix}index_enwiki-latest.db", 
            "db1_path": f"{CONFIG.local_prefix}db1.rocks/",  
            "db1_rev_path": f"{CONFIG.local_prefix}db1_rev.rocks/",  
        },
        outlets=[ROCKS_DB_1, ROCKS_DB_1_REVERSE]
        #start_date=datetime(3021, 1, 1),
    )
    create_reverse.doc_md = dedent(
        """\
    #### Task Documentation
    The task creates a mapping from Wikidata to Wikipedia and from Wikipedia to Wikidata.
    """
    )
