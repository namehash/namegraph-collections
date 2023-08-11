from datetime import datetime, timedelta
from textwrap import dedent
from rocksdict import AccessType, Rdict
import rocksdict
from tqdm import tqdm
import json, re, sys, csv, jsonlines
from kwnlp_sql_parser import WikipediaSqlDump
from urllib.parse import unquote, quote
from wikimapper.mapper import WikiMapper

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from functools import lru_cache

from create_inlets import CONFIG, WIKIPEDIA_CATEGORYLINKS, WIKIPEDIA_PAGELINKS, WIKIMAPPER, CollectionDataset
from create_kv import ROCKS_DB_3, ROCKS_DB_1_REVERSE, ROCKS_DB_1, ROCKS_DB_6, ROCKS_DB_2

CATEGORIES = CollectionDataset(f"{CONFIG.remote_prefix}categories.json")
ALLOWED_CATEGORIES = CollectionDataset(f"{CONFIG.remote_prefix}allowed-categories.txt")
CATEGORY_PAGES = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-categories.csv")
MAPPED_CATEGORIES = CollectionDataset(f"{CONFIG.remote_prefix}mapped-categories.csv")
SORTED_CATEGORIES = CollectionDataset(f"{CONFIG.remote_prefix}sorted-categories.csv")
CATEGORY_MEMBERS = CollectionDataset(f"{CONFIG.remote_prefix}category_members.jsonl")
VALIDATED_CATEGORY_MEMBERS = CollectionDataset(f"{CONFIG.remote_prefix}validated_category_members.jsonl")

LISTS = CollectionDataset(f"{CONFIG.remote_prefix}lists.json")
ALLOWED_LISTS = CollectionDataset(f"{CONFIG.remote_prefix}allowed-lists.txt")
LIST_PAGES = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-pagelinks.csv")
MAPPED_LISTS = CollectionDataset(f"{CONFIG.remote_prefix}mapped-lists.csv")
SORTED_LISTS = CollectionDataset(f"{CONFIG.remote_prefix}sorted-lists.csv")
LIST_MEMBERS = CollectionDataset(f"{CONFIG.remote_prefix}list_members.jsonl")
VALIDATED_LIST_MEMBERS = CollectionDataset(f"{CONFIG.remote_prefix}validated_list_members.jsonl")


def extract_collections(id_title_path: str, members_type_path: str, mode: str, output: str):
    id_title_db = Rdict(id_title_path, access_type=AccessType.read_only())
    members_type_db = Rdict(members_type_path, access_type=AccessType.read_only())


    if mode == 'category':
        predicate = 'category_contains'
    elif mode == 'list':
        predicate = 'is_a_list_of'

    # there might more than one type of list/category

    articles = []
    for wikidata_id, predicates in tqdm(members_type_db.items()):
        try:
            if predicate in predicates:
                article_name = id_title_db[wikidata_id]
                
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
        wiki_id = wikimapper.title_to_wikipedia_id(re.sub(r" ", "_", unquote(list_obj['article'])))
        if wiki_id is not None:
            page_ids.append(wiki_id)
        else:
            print('Missing', list_obj['article'], file=sys.stderr)

    with open(output, 'w', encoding='utf-8') as f:
        f.write('\n'.join(map(str, page_ids)) + '\n')


with DAG(
    "categories",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
    },
    description="Tasks related to the creation of categories",
    schedule=[ROCKS_DB_1_REVERSE, ROCKS_DB_3],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["categories", "collection-templates"],
) as dag:
    create_categories_task = PythonOperator(
        task_id='create-categories',
        python_callable=extract_collections,
        op_kwargs={
            "id_title_path": ROCKS_DB_1_REVERSE.local_name(), 
            "members_type_path": ROCKS_DB_3.local_name(), 
            "mode": 'category', 
            "output": CATEGORIES.local_name()
        },
        outlets=[CATEGORIES]
    )
    create_categories_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create categories JSON file.

    This file contains the Wikidata items that are categories in the English Wikipedia.
    """
    )

    create_allowed_categories_task = PythonOperator(
        task_id='create-allowed-categories',
        python_callable=extract_titles,
        op_kwargs={
            "input": CATEGORIES.local_name(),
            "output": ALLOWED_CATEGORIES.local_name(),
        },
        outlets=[ALLOWED_CATEGORIES]
    )
    create_allowed_categories_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create allowed categories TXT file.

    This file contains the titles of the categories from the English Wikipedia that are registered in Wikidata.
    """
    )

    create_categories_task >> create_allowed_categories_task

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
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
    },
    description="Tasks related to the creation of categories from English Wikipedia",
    schedule=[ALLOWED_CATEGORIES, WIKIPEDIA_CATEGORYLINKS],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["categories", "collection-templates"],
) as dag:
    copy_enwiki_categories_task = BashOperator(
        task_id="copy-enwiki-categories",
        bash_command=f"cp {WIKIPEDIA_CATEGORYLINKS.latest_local_name()} {WIKIPEDIA_CATEGORYLINKS.current_local_name()}",
    )
    copy_enwiki_categories_task.doc_md = dedent(
        """\
    #### Task Documentation
    Copy enwiki categories with current date.
    """
    )

    create_categories_task = PythonOperator(
        task_id='create-category-links',
        python_callable=extract_associations_from_dump,
        op_kwargs={
            "input": WIKIPEDIA_CATEGORYLINKS.current_local_name(), 
            "mode": 'category', 
            "output": CATEGORY_PAGES.local_name(),
            "allowed_values": ALLOWED_CATEGORIES.local_name(),
        },
        outlets=[CATEGORY_PAGES],
    )
    create_categories_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create file with category content.

    The file contains associations between English Wikipedia categories and the pages that belong to those categories.
    """
    )

    copy_enwiki_categories_task >> create_categories_task

with DAG(
    "lists",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
    },
    description="Tasks related to the creation of lists",
    schedule=[ROCKS_DB_1_REVERSE, ROCKS_DB_3, WIKIMAPPER],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["lists", "collection-templates"],
) as dag:
    create_lists_task = PythonOperator(
        task_id='create-lists',
        python_callable=extract_collections,
        op_kwargs={
            "id_title_path": ROCKS_DB_1_REVERSE.local_name(), 
            "members_type_path": ROCKS_DB_3.local_name(), 
            "mode": 'list', 
            "output": LISTS.local_name(),
        },
        outlets=[LISTS],
    )
    create_lists_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create list JSON file.

    This file contains the Wikidata items that are lists in the English Wikipedia.
    """
    )

    create_allowed_lists_task = PythonOperator(
        task_id='create-allowed-lists',
        python_callable=extract_page_ids,
        op_kwargs={
            "input": LISTS.local_name(),
            "output": ALLOWED_LISTS.local_name(),
            "wikimapper_path": WIKIMAPPER.local_name()
        },
        outlets=[ALLOWED_LISTS]
    )
    create_allowed_lists_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create allowed lists TXT file.

    This file contains the titles of the lists from the English Wikipedia that are registered in Wikidata.
    """
    )

    create_lists_task >> create_allowed_lists_task

with DAG(
    "lists-enwiki",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
    },
    description="Tasks related to the creation of lists from English Wikipedia",
    schedule=[ALLOWED_LISTS, WIKIPEDIA_PAGELINKS],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["lists", "collection-templates"],
) as dag:
    copy_enwiki_lists_task = BashOperator(
        task_id="copy-enwiki-lists",
        bash_command=f"cp {WIKIPEDIA_PAGELINKS.latest_local_name()} {WIKIPEDIA_PAGELINKS.current_local_name()}",
    )
    copy_enwiki_lists_task.doc_md = dedent(
        """\
    #### Task Documentation
    Copy enwiki lists with current date.
    """
    )

    create_enwiki_lists_task = PythonOperator(
        task_id='create-list-links',
        python_callable=extract_associations_from_dump,
        op_kwargs={
            "input": WIKIPEDIA_PAGELINKS.current_local_name(), 
            "mode": 'list', 
            "output": LIST_PAGES.local_name(),
            "allowed_values": ALLOWED_LISTS.local_name(),
        },
        outlets=[LIST_PAGES]
    )
    create_enwiki_lists_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create file with list pages content.

    The file contains associations between English Wikipedia list pages and the pages that belong to those lists.
    """
    )

    copy_enwiki_lists_task >> create_enwiki_lists_task


def title_for(wikipedia_id: int, wikipedia_id2title, mapper) -> str:
    if (title := wikipedia_id2title.get(wikipedia_id)) is None:
        title = mapper.wikipedia_id_to_title(wikipedia_id)
        wikipedia_id2title[wikipedia_id] = title

    return title



def map_to_titles(input, output, mode, wikimapper_path):
    mapper = WikiMapper(wikimapper_path)
    wikipedia_id2title: dict[int, str] = dict()

    skipped = 0
    with open(input, 'r', encoding='utf-8') as in_csv, open(output, 'w', encoding='utf-8') as out_csv:
        reader = csv.reader(in_csv, delimiter=',')
        writer = csv.writer(out_csv, delimiter=',')

        header = next(reader)
        writer.writerow(['collection_title', 'member_title'])

        for line in tqdm(reader):
            if mode == 'category':
                collection_title = line[1]

                member_wikipedia_id = int(line[0])
                member_title = title_for(member_wikipedia_id, wikipedia_id2title, mapper)

            elif mode == 'list':
                list_wikipedia_id = int(line[0])
                collection_title = title_for(list_wikipedia_id, wikipedia_id2title, mapper)

                member_title = line[1]

            else:
                raise ValueError(f'invalid mode - {mode}')

            if collection_title and member_title:
                writer.writerow([collection_title, member_title])
            else:
                skipped += 1

    print('skipped', skipped)

with DAG(
    "mapped-categories",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
    },
    description="Mapping of category and member items to titles",
    schedule=[CATEGORY_PAGES, WIKIMAPPER],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["categories", "collection-templates"],
) as dag:
    create_mapped_categories_task = PythonOperator(
        task_id='create-mapped-categories',
        python_callable=map_to_titles,
        op_kwargs={
            "input": CATEGORY_PAGES.local_name(), 
            "mode": 'category', 
            "output": MAPPED_CATEGORIES.local_name(),
            "wikimapper_path": WIKIMAPPER.local_name(),
        },
    )
    create_mapped_categories_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create file with the mapped categories.

    The resulting file contains a mapping from a collection to the member with both the collection and the member referenced by title.
    """
    )

    create_sorted_categories_task = BashOperator(
        outlets=[SORTED_CATEGORIES],
        task_id="sort-categories",
        bash_command=f"(head -n 1 {MAPPED_CATEGORIES.local_name()} && tail -n +2 {MAPPED_CATEGORIES.local_name()} | " + 
            f"LC_ALL=C sort) > {SORTED_CATEGORIES.local_name()}",
    )

    create_sorted_categories_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create file with the sorted categories.

    The resulting file contains a mapping from a collection to the member sorted by the collection name.
    """
    )

    create_mapped_categories_task >> create_sorted_categories_task

with DAG(
    "mapped-lists",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
    },
    description="Mapping of list and member item to titles",
    schedule=[LIST_PAGES, WIKIMAPPER],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["lists", "collection-templates"],
) as dag:
    create_mapped_lists_task = PythonOperator(
        task_id='create-mapped-lists',
        python_callable=map_to_titles,
        op_kwargs={
            "input": LIST_PAGES.local_name(), 
            "mode": 'list', 
            "output": MAPPED_LISTS.local_name(),
            "wikimapper_path": WIKIMAPPER.local_name(),
        },
    )
    create_mapped_lists_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create file with the mapped lists.

    The resulting file contains a mapping from a collection to the member with both the collection and the member referenced by title.
    """
    )

    create_sorted_lists_task = BashOperator(
        outlets=[SORTED_LISTS],
        task_id="sort-lists",
        bash_command=f"(head -n 1 {MAPPED_LISTS.local_name()} && tail -n +2 {MAPPED_LISTS.local_name()} | " + 
            f"LC_ALL=C sort) > {SORTED_LISTS.local_name()}",
    )

    create_sorted_lists_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create file with the sorted lists.

    The resulting file contains a mapping from a collection to the member sorted by the collection name.
    """
    )

    create_mapped_lists_task >> create_sorted_lists_task

def write_item(collections, members, writer, key):
    try:
        item = collections[key]
        writer.write({
            'item': item['item'],
            'type': item['type'],
            'article': item['article'],
            'members': [m.replace('_', ' ') for m in members]
        })
    except KeyError as ex:
        print('Missing:', key, file=sys.stderr)


def reformat_csv_to_json(input, output, collections_json):
    with open(collections_json, 'r', encoding='utf-8') as f:
        collections = {
            unquote(coll['article'].removeprefix('Category:')): coll
            #re.sub(r"_", " ", unquote(coll['article'].removeprefix('Category:'))): coll
            for coll in json.load(f)
        }

    with open(input, 'r', encoding='utf-8') as csvfile, jsonlines.open(output, 'w') as writer:
        reader = csv.reader(csvfile, delimiter=',')

        _header = next(reader)
        first_row = next(reader)

        print(list(collections.keys())[:10])
        prev_key = first_row[0]
        prev_key = re.sub(r"_", " ", prev_key) 
        members = [first_row[1]]
        for key, member in tqdm(reader):
            key = re.sub(r"_", " ", key)
            if key != prev_key:
                write_item(collections, members, writer, prev_key)
                prev_key = key
                members = []

            members.append(member)

        write_item(collections, members, writer, key)

NO_PARENT = 0

class ParentFinder():
    def __init__(self, id_type_db, same_as_db):
        self.id_type_db = id_type_db
        self.same_as_db = same_as_db

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return self.id == other.id

    @lru_cache(maxsize=None)
    def has_path_rocksdb_subclass(self, source: str, target: str) -> bool:
        global NO_PARENT
        visited = set()
        stack: list[str] = [source]
        visited.add(source)

        while stack:
            curr = stack.pop()
            if curr == target:
                return True
            try:
                neighbours = self.id_type_db[curr].get('subclass_of', [])
            except KeyError:
                try:
                    neighbours = self.id_type_db[self.same_as_db[curr]['same_as'][0]].get('subclass_of', [])  # try redirect
                except KeyError:
                    NO_PARENT += 1
                    if curr in self.same_as_db and self.same_as_db[curr]["same_as"][0] in self.same_as_db:
                        print(f'redirect KeyError: {curr} {self.same_as_db[curr]["same_as"][0]}', file=sys.stderr)
                    continue
            for neigh in neighbours:
                if neigh in visited:
                    continue

                if neigh == target:
                    return True

                visited.add(neigh)
                stack.append(neigh)

        return False


def has_path_rocksdb(source: str, target: str, id_type_db: Rdict, same_as_db: Rdict, finder: ParentFinder) -> bool:
    global NO_PARENT
    entries = []
    try:
        entries += id_type_db[source].get('instance_of', [])
        entries += id_type_db[source].get('subclass_of', [])
    except KeyError:
        try:
            entries += id_type_db[same_as_db[source]['same_as'][0]].get('instance_of', [])
            entries += id_type_db[same_as_db[source]['same_as'][0]].get('subclass_of', [])
        except KeyError:
            NO_PARENT += 1
        pass
    return any([finder.has_path_rocksdb_subclass(entry, target) for entry in entries])
    

def extract_article_name(article: str) -> str:
    """
    Extracts article name from the link.
    :param article: link to the article
    :return: article name
    """
    if (not article.startswith('http://')) and (not article.startswith('https://')):
        return article

    m = re.match(r'https?://en\.wikipedia\.org/wiki/(.+)', article)
    return m.group(1)

def extract_id(link: str) -> str:
    # assert link.startswith('http://www.wikidata.org/entity/Q')
    if link.startswith('http://www.wikidata.org/entity/Q'):
        return link[len('http://www.wikidata.org/entity/'):]
    return link

def extract_ids(links: list[str]) -> list[str]:
    return [extract_id(link) for link in links if link]


def validate_members(input: str, output: str, title_id_path: str, id_type_path: str, same_as_path: str, wikimapper_path: str):
    id_type_db = rocksdict.Rdict(id_type_path, access_type=AccessType.read_only())
    same_as_db = rocksdict.Rdict(same_as_path, access_type=AccessType.read_only())
    title_id_db = rocksdict.Rdict(title_id_path, access_type=AccessType.read_only())

    mapper = WikiMapper(wikimapper_path)
    finder = ParentFinder(id_type_db, same_as_db)

    count_valid_members = 0
    count_invalid_members = 0

    with jsonlines.open(input) as reader, jsonlines.open(output, mode='w') as writer:
        for obj in tqdm(reader):
            collection_item = obj['item']
            collection_types = obj['type']
            collection_type_ids = extract_ids(collection_types)
            collection_article = extract_article_name(obj['article'])
            members = obj['members']

            valid_members = []
            for member in members:
                article_name = extract_article_name(member)

                try:
                    article_wikidata_id = title_id_db[quote(member.replace(' ', '_'))]['about'] #TODO: what about comma?
                except KeyError:
                    article_wikidata_id = mapper.title_to_id(member.replace(' ', '_'))
                    if article_wikidata_id is None:
                        continue

                article_is_valid = False
                for collection_type_id in collection_type_ids:
                    if has_path_rocksdb(article_wikidata_id, collection_type_id, id_type_db, same_as_db, finder):
                        article_is_valid = True
                        break

                if article_is_valid:
                    valid_members.append((article_wikidata_id, article_name))
                    count_valid_members += 1
                else:
                    count_invalid_members += 1

            writer.write({
                'item': extract_id(collection_item),
                'type': collection_type_ids,
                'article': collection_article,
                'members': valid_members,
                'valid_members_count': len(valid_members),
                'invalid_members_count': len(members) - len(valid_members)
            })

        print('Members', count_valid_members, 'valid,', count_invalid_members, 'invalid')

    print('No parent', NO_PARENT)


with DAG(
    "category-members",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
    },
    description="Change category associations to JSON format",
    schedule=[CATEGORIES, SORTED_CATEGORIES, ROCKS_DB_1, ROCKS_DB_2, ROCKS_DB_6, WIKIMAPPER],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["categories", "collection-templates"],
) as dag:
    create_category_members_task = PythonOperator(
        task_id='create-category-members',
        python_callable=reformat_csv_to_json,
        op_kwargs={
            "input": SORTED_CATEGORIES.local_name(), 
            "output": CATEGORY_MEMBERS.local_name(),
            "collections_json": CATEGORIES.local_name(),
        },
    )
    create_category_members_task.doc_md = dedent(
        """\
    #### Task Documentation
    Change CSV **categories** file, to JSON format.

    The task combines the Wikipedia names with Wikidata structured info resulting in a JSON object containing the info about the 
    collection and its members.
    """
    )


    validate_category_members_task = PythonOperator(
        task_id='validate-category-members',
        python_callable=validate_members,
        op_kwargs={
            "input": CATEGORY_MEMBERS.local_name(), 
            "output": VALIDATED_CATEGORY_MEMBERS.local_name(),
            "title_id_path": ROCKS_DB_1.local_name(),
            "id_type_path": ROCKS_DB_2.local_name(),
            "same_as_path": ROCKS_DB_6.local_name(),
            "wikimapper_path": WIKIMAPPER.local_name(),
        },
        outlets=[VALIDATED_CATEGORY_MEMBERS],
    )

    validate_category_members_task.doc_md = dedent(
        """\
    #### Task Documentation
    Validate correctnes of **category** members.

    The methods checks if the members of a collection have type compatible with the collection's type.
    """
    )

    create_category_members_task >> validate_category_members_task


with DAG(
    "list-members",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
    },
    description="Change list associations to JSON format",
    schedule=[LISTS, SORTED_LISTS, ROCKS_DB_1, ROCKS_DB_2, ROCKS_DB_6, WIKIMAPPER],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["lists", "collection-templates"],
) as dag:
    create_list_members_task = PythonOperator(
        task_id='create-list-members',
        python_callable=reformat_csv_to_json,
        op_kwargs={
            "input": SORTED_LISTS.local_name(), 
            "output": LIST_MEMBERS.local_name(),
            "collections_json": LISTS.local_name(),
        },
    )
    create_list_members_task.doc_md = dedent(
        """\
    #### Task Documentation
    Change CSV **lists** file, to JSON format.

    The task combines the Wikipedia names with Wikidata structured info resulting in a JSON object containing the info about the 
    collection and its members.
    """
    )


    validate_list_members_task = PythonOperator(
        task_id='validate-list-members',
        python_callable=validate_members,
        op_kwargs={
            "input": LIST_MEMBERS.local_name(), 
            "output": VALIDATED_LIST_MEMBERS.local_name(),
            "title_id_path": ROCKS_DB_1.local_name(),
            "id_type_path": ROCKS_DB_2.local_name(),
            "same_as_path": ROCKS_DB_6.local_name(),
            "wikimapper_path": WIKIMAPPER.local_name(),
        },
        outlets=[VALIDATED_LIST_MEMBERS],
    )

    validate_list_members_task.doc_md = dedent(
        """\
    #### Task Documentation
    Validate correctnes of **list** members.

    The methods checks if the members of a collection have type compatible with the collection's type.
    """
    )

    create_list_members_task >> validate_list_members_task

