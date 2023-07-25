from datetime import datetime, timedelta
from textwrap import dedent
from make_dag import CONFIG, WIKIPEDIA_CATEGORYLINKS, WIKIPEDIA_PAGELINKS, WIKIMAPPER
from create_kv import ROCKS_DB_3, ROCKS_DB_1_REVERSE
from rocksdict import AccessType
import rocksdict
from tqdm import tqdm
import json, re, sys
from kwnlp_sql_parser import WikipediaSqlDump
from urllib.parse import unquote
from wikimapper.mapper import WikiMapper

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

CATEGORIES = Dataset(f"{CONFIG.remote_prefix}categories.json")
ALLOWED_CATEGORIES = Dataset(f"{CONFIG.remote_prefix}allowed-categories.txt")
CATEGORY_PAGES = Dataset(f"{CONFIG.remote_prefix}enwiki-categories.csv")

LISTS = Dataset(f"{CONFIG.remote_prefix}lists.json")
ALLOWED_LISTS = Dataset(f"{CONFIG.remote_prefix}allowed-lists.txt")
LIST_PAGES = Dataset(f"{CONFIG.remote_prefix}enwiki-pagelinks.csv")


def extract_collections(db1_rev_path: str, db3_path: str, mode: str, output: str):

    db1 = rocksdict.Rdict(db1_rev_path, access_type=AccessType.read_only())
    db3 = rocksdict.Rdict(db3_path, access_type=AccessType.read_only())

    # for wikidata_id, predicates in db1.items():
    #     print(wikidata_id, predicates)

    if mode == 'category':
        predicate = 'category_contains'
    elif mode == 'list':
        predicate = 'is_a_list_of'

    # there might more than one type of list/category

    articles = []
    for wikidata_id, predicates in tqdm(db3.items()):
        try:
            if predicate in predicates:
                article_name = db1[wikidata_id]
                
                if mode == 'category':
                    if not article_name.startswith('Category:'):
                        continue
                elif mode == 'list':
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
    json.dump(articles, open(output, 'w', encoding='utf-8'), indent=2, ensure_ascii=False)

def extract_titles(input, output):
    with open(input, 'r', encoding='utf-8') as f:
        categories = json.load(f)

    titles = [category['article'] for category in categories]

    with open(output, 'w', encoding='utf-8') as f:
        f.write('\n'.join(titles) + '\n')


def extract_page_ids(input, output, wikimapper_path):
    with open(input, 'r', encoding='utf-8') as f:
        lists = json.load(f)

    wikimapper = WikiMapper(wikimapper_path)

    page_ids = []
    for list_obj in lists:
        wiki_id = wikimapper.title_to_wikipedia_id(unquote(list_obj['article']))
        if wiki_id is not None:
            page_ids.append(wiki_id)
        else:
            print('Missing', list_obj['article'], file=sys.stderr)

    with open(output, 'w', encoding='utf-8') as f:
        f.write('\n'.join(map(str, page_ids)) + '\n')


with DAG(
    "categories",
    default_args={
        "depends_on_past": False,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "cwd": CONFIG.local_prefix,
    },
    description="Tasks related to the creation of categories",
    schedule=[ROCKS_DB_1_REVERSE, ROCKS_DB_3],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["categories", "collection-templates"],
) as dag:
    create_categories = PythonOperator(
        task_id='create-categories',
        python_callable=extract_collections,
        op_kwargs={
            "db1_rev_path": f"{CONFIG.local_prefix}db1_rev.rocks", 
            "db3_path": f"{CONFIG.local_prefix}db3.rocks", 
            "mode": 'category', 
            "output": f"{CONFIG.local_prefix}categories.json"
        },
        outlets=[CATEGORIES]
        #start_date=datetime(3021, 1, 1),
    )
    create_categories.doc_md = dedent(
        """\
    #### Task Documentation
    Create categories JSON file.

    This file contains the Wikidata items that are categories in the English Wikipedia.
    """
    )

    create_allowed_categories = PythonOperator(
        task_id='create-allowed-categories',
        python_callable=extract_titles,
        op_kwargs={
            "input": f"{CONFIG.local_prefix}categories.json",
            "output": f"{CONFIG.local_prefix}allowed-categories.txt"
        },
        outlets=[ALLOWED_CATEGORIES]
        #start_date=datetime(3021, 1, 1),
    )
    create_allowed_categories.doc_md = dedent(
        """\
    #### Task Documentation
    Create allowed categories TXT file.

    This file contains the titles of the categories from the English Wikipedia that are registered in Wikidata.
    """
    )

    create_categories >> create_allowed_categories

def extract_associations_from_dump(input, output, mode, allowed_values):
    if mode == 'category':
        def clean_id(id):
            return re.sub(r" ", "_", unquote(id.strip().removeprefix('Category:')))
        column_names = ('cl_from', 'cl_to')
        filter_column = 'cl_to'
    elif mode == 'list':
        def clean_id(id):
            return id.strip()
        column_names = ('pl_from', 'pl_title')
        filter_column = 'pl_from'
    else:
        raise ValueError('either `categorylinks` or `pagelinks` flag must be set')


    with open(allowed_values, 'r', encoding='utf-8') as f:
        allowed_items = tuple([clean_id(id) for id in f.read().strip('\n').split('\n') ])

    WikipediaSqlDump(
        input,
        keep_column_names=column_names,
        allowlists={filter_column: allowed_items}
    ).to_csv(output)

with DAG(
    "categories-enwiki",
    default_args={
        "depends_on_past": False,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "cwd": CONFIG.local_prefix,
    },
    description="Tasks related to the creation of categories from English Wikipedia",
    schedule=[ALLOWED_CATEGORIES, WIKIPEDIA_CATEGORYLINKS],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["categories", "collection-templates"],
) as dag:
    create_categories = PythonOperator(
        task_id='create-category-links',
        python_callable=extract_associations_from_dump,
        op_kwargs={
            "input": f"{CONFIG.local_prefix}enwiki-20230720-categorylinks.sql.gz", 
            "mode": 'category', 
            "output": f"{CONFIG.local_prefix}enwiki-categories.csv",
            "allowed_values": f"{CONFIG.local_prefix}allowed-categories.txt",
        },
        outlets=[CATEGORY_PAGES]
        #start_date=datetime(3021, 1, 1),
    )
    create_categories.doc_md = dedent(
        """\
    #### Task Documentation
    Create file with category content.

    The file contains associations between English Wikipedia categories and the pages that belong to those categories.
    """
    )

with DAG(
    "lists",
    default_args={
        "depends_on_past": False,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "cwd": CONFIG.local_prefix,
    },
    description="Tasks related to the creation of lists",
    schedule=[ROCKS_DB_1_REVERSE, ROCKS_DB_3, WIKIMAPPER],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["lists", "collection-templates"],
) as dag:
    create_lists = PythonOperator(
        task_id='create-lists',
        python_callable=extract_collections,
        op_kwargs={
            "db1_rev_path": f"{CONFIG.local_prefix}db1_rev.rocks", 
            "db3_path": f"{CONFIG.local_prefix}db3.rocks", 
            "mode": 'list', 
            "output": f"{CONFIG.local_prefix}lists.json"
        },
        outlets=[LISTS],
        #start_date=datetime(3021, 1, 1),
    )
    create_lists.doc_md = dedent(
        """\
    #### Task Documentation
    Create list JSON file.

    This file contains the Wikidata items that are lists in the English Wikipedia.
    """
    )

    create_allowed_lists = PythonOperator(
        task_id='create-allowed-lists',
        python_callable=extract_page_ids,
        op_kwargs={
            "input": f"{CONFIG.local_prefix}lists.json",
            "output": f"{CONFIG.local_prefix}allowed-lists.txt",
            "wikimapper_path": f"{CONFIG.local_prefix}index_enwiki-latest.db"
        },
        outlets=[ALLOWED_LISTS]
        #start_date=datetime(3021, 1, 1),
    )
    create_allowed_lists.doc_md = dedent(
        """\
    #### Task Documentation
    Create allowed lists TXT file.

    This file contains the titles of the lists from the English Wikipedia that are registered in Wikidata.
    """
    )

    create_lists >> create_allowed_lists

with DAG(
    "lists-enwiki",
    default_args={
        "depends_on_past": False,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "cwd": CONFIG.local_prefix,
    },
    description="Tasks related to the creation of lists from English Wikipedia",
    schedule=[ALLOWED_LISTS, WIKIPEDIA_PAGELINKS],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["lists", "collection-templates"],
) as dag:
    create_categories = PythonOperator(
        task_id='create-list-links',
        python_callable=extract_associations_from_dump,
        op_kwargs={
            "input": f"{CONFIG.local_prefix}enwiki-20230720-pagelinks.sql.gz", 
            "mode": 'list', 
            "output": f"{CONFIG.local_prefix}enwiki-lists.csv",
            "allowed_values": f"{CONFIG.local_prefix}allowed-lists.txt",
        },
        outlets=[LIST_PAGES]
        #start_date=datetime(3021, 1, 1),
    )
    create_categories.doc_md = dedent(
        """\
    #### Task Documentation
    Create file with list pages content.

    The file contains associations between English Wikipedia list pages and the pages that belong to those lists.
    """
    )