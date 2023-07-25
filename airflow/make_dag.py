
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, Dataset

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

from dataclasses import dataclass

@dataclass
class Config:
    email: str
    local_prefix: str
    remote_prefix: str
    date_str: str
    start_date: datetime


CONFIG=Config(
    "apohllo@o2.pl",
    "/home/apohllo/airflow/dags/",
    "file:///home/apohllo/src/airflow/dags/",
    datetime.now().strftime("%Y%m%d"),
    datetime(2021, 1, 1)
)

def wget_for_wikidata(type: str):
    return f"https://dumps.wikimedia.org/wikidatawiki/entities/latest-{type}.nt.bz2"

WIKIDATA_TRUTHY = Dataset(f"{CONFIG.remote_prefix}latest-truthy.nt.bz2")

with DAG(
    "download-wikidata",
    default_args={
        "cwd": CONFIG.local_prefix,
        "depends_on_past": False,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Tasks related to downloading Wikidata source files",
    schedule=timedelta(days=1),
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["download", "collection-templates"],
) as dag:

    dag.doc_md = """
    Downloading of Wikidata files
    """  

    pagelinks_task = BashOperator(
        outlets=[WIKIDATA_TRUTHY],
        task_id="download-truthy",
        bash_command=f"wget {wget_for_wikidata('truthy')} -O {CONFIG.local_prefix}latest-truthy.nt.bz2",
        start_date=datetime(3021, 1, 1),
    )

    pagelinks_task.doc_md = dedent(
        """\
    #### Task Documentation
    Download the latest **thruthy** statements from Wikidata.
    """
    )

WIKIDATA_FILTERED = Dataset(f"{CONFIG.remote_prefix}latest-truthy.filtered.nt.bz2")

with DAG(
    "filter-wikidata",
    default_args={
        "depends_on_past": False,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Tasks related to processing of source Wikidata files",
    schedule=[WIKIDATA_TRUTHY],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["filter", "collection-templates"],
) as dag:

    regex = "'^(<https://en\.wikipedia\.org/wiki/|<http://www\.wikidata\.org/entity/).*((<http://www\.wikidata\.org/prop/direct/P18>|<http://www\.wikidata\.org/prop/direct/P1753>|<http://www\.wikidata\.org/prop/direct/P31>|<http://schema\.org/about>|<http://www\.wikidata\.org/prop/direct/P1754>|<http://www\.wikidata\.org/prop/direct/P4224>|<http://www\.wikidata\.org/prop/direct/P948>|<http://www\.wikidata\.org/prop/direct/P279>|<http://www\.wikidata\.org/prop/direct/P360>|<http://www\.w3\.org/2002/07/owl\#sameAs>)|((<http://schema\.org/description>|<http://schema\.org/name>|<http://www\.w3\.org/2000/01/rdf\-schema\#label>).*@en .$$))'"
    t1 = BashOperator(
        outlets=[WIKIDATA_FILTERED],
        task_id="grep-wikidata",
        cwd=f"{CONFIG.local_prefix}",
        bash_command=f"lbzip2 -d latest-truthy.nt.bz2 --stdout | grep -E {regex} | lbzip2 -c > latest-truthy.filtered.nt.bz2 ",
    )

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    This task filters the predicates in the Wikipedia dump to include 
    only a subset of predicates and and a subset of entities (only wikidata and English wikipedia).
    """
    )

    dag.doc_md = """
    Filtering Wikidata for entries including only specific subjects and predicates.
    """  

WIKIPEDIA_PAGELINKS = Dataset(f"{CONFIG.remote_prefix}enwiki-{CONFIG.date_str}-pagelinks.sql.gz")
WIKIPEDIA_REDIRECT = Dataset(f"{CONFIG.remote_prefix}enwiki-latest-redirect.sql.gz")
WIKIPEDIA_CATEGORYLINKS = Dataset(f"{CONFIG.remote_prefix}enwiki-{CONFIG.date_str}-categorylinks.sql.gz")
WIKIPEDIA_PAGEPROPS = Dataset(f"{CONFIG.remote_prefix}enwiki-latest-page_pros.sql.gz")
WIKIPEDIA_PAGE = Dataset(f"{CONFIG.remote_prefix}enwiki-latest-page.sql.gz")

def wget_for_wikipedia(type: str, latest: bool=False):
    if latest:
        infix = "latest"
    else:
        infix = CONFIG.date_str
    return f"https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-{type}.sql.gz -O {CONFIG.local_prefix}enwiki-{infix}-{type}.sql.gz"

with DAG(
    "download-wikipedia",
    default_args={
        "cwd": CONFIG.local_prefix,
        "depends_on_past": False,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Tasks related to downloading Wikipedia source files",
    schedule=timedelta(days=1),
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["download", "collection-templates"],
) as dag:

    dag.doc_md = """
    Downloading of Wikipedia dump files
    """  
    print(WIKIPEDIA_PAGELINKS.uri)


    pagelinks_task = BashOperator(
        outlets=[WIKIPEDIA_PAGELINKS],
        task_id="download-pagelinks",
        #bash_command=f"wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pagelinks.sql.gz -O {CONFIG.local_prefix}enwiki-{CONFIG.date_str}-pagelinks.sql.gz",
        bash_command=f"wget {wget_for_wikipedia('pagelinks')}",
        start_date=datetime(3021, 1, 1),
    )

    pagelinks_task.doc_md = dedent(
        """\
    #### Task Documentation
    Download the latest **pagelinks** file and reaname it for the current date.
    The current date requirement is set, because this is the requirement of the file parser.
    """
    )


    redirect_task = BashOperator(
        outlets=[WIKIPEDIA_REDIRECT],
        task_id="download-redirect",
        bash_command=f"wget {wget_for_wikipedia('redirect', latest=True)}",
        #start_date=datetime(3021, 1, 1),
    )

    redirect_task.doc_md = dedent(
        """\
    #### Task Documentation
    Download the latest **redirects** file.
    """
    )

    categorylinks_task = BashOperator(
        outlets=[WIKIPEDIA_CATEGORYLINKS],
        task_id="download-categorylinks",
        bash_command=f"wget {wget_for_wikipedia('categorylinks')}",
        start_date=datetime(3021, 1, 1),
    )

    categorylinks_task.doc_md = dedent(
        """\
    #### Task Documentation
    Download the latest **category links** file and reaname it for the current date.
    """
    )

    pageprops_task = BashOperator(
        outlets=[WIKIPEDIA_PAGEPROPS],
        task_id="download-pageprops",
        bash_command=f"wget {wget_for_wikipedia('page_props')}",
        start_date=datetime(3021, 1, 1),
    )

    # TODO make copy of page props for mapper
    pageprops_task.doc_md = dedent(
        """\
    #### Task Documentation
    Download the latest **page props** file and reaname it for the current date.
    """
    )

    page_task = BashOperator(
        outlets=[WIKIPEDIA_PAGE],
        task_id="download-page",
        bash_command=f"wget {wget_for_wikipedia('page', latest=True)}",
        start_date=datetime(3021, 1, 1),
    )

    page_task.doc_md = dedent(
        """\
    #### Task Documentation
    Download the latest **page** file.
    """
    )

WIKIMAPPER = Dataset(f"{CONFIG.remote_prefix}index_enwiki-latest.db")

with DAG(
    "wikimapper",
    default_args={
        "cwd": CONFIG.local_prefix,
        "depends_on_past": False,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Tasks related to running the wikimapper service",
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["db", "collection-templates"],
    schedule=[WIKIPEDIA_REDIRECT, WIKIPEDIA_PAGE, WIKIPEDIA_PAGEPROPS],
) as dag:
    dag.doc_md = """
    Loading of Wikipedia dump files to wikimapper database
    """  


    pagelinks_task = BashOperator(
        outlets=[WIKIMAPPER],
        task_id="load-redirect",
        bash_command=f"wikimapper create enwiki-latest --dumpdir {CONFIG.local_prefix} --target index_enwiki-latest.db",
    )

    pagelinks_task.doc_md = dedent(
        """\
    #### Task Documentation
    Load the redirects to the wikimapper database.
    """
    )
