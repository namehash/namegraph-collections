
from datetime import datetime, timedelta
from textwrap import dedent
import re

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
    run_interval: timedelta


CONFIG=Config(
    "apohllo@o2.pl",
    "/home/apohllo/airflow/dags/data/",
    "file:///home/apohllo/src/airflow/dags/",
    datetime.now().strftime("%Y%m%d"),
    datetime(2021, 1, 1),
    timedelta(weeks=4)
)

class CollectionDataset(Dataset):
    def name(self):
        return self.uri.split("/")[-1]

    def latest_name(self):
        return re.sub(f"-{datetime.now().strftime('%Y%m%d')}-", "-latest-", self.name())

    def current_name(self):
        return re.sub("-latest-", f"-{datetime.now().strftime('%Y%m%d')}-", self.name())

    def local_name(self):
        return CONFIG.local_prefix + self.name()

    def current_local_name(self):
        return CONFIG.local_prefix + self.current_name()

    def latest_local_name(self):
        return CONFIG.local_prefix + self.latest_name()
        



WIKIDATA_TRUTHY = CollectionDataset(f"{CONFIG.remote_prefix}latest-truthy.nt.bz2")
WIKIDATA_FILTERED = CollectionDataset(f"{CONFIG.remote_prefix}latest-truthy.filtered.nt.bz2")
WIKIPEDIA_PAGELINKS = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-latest-pagelinks.sql.gz")
WIKIPEDIA_REDIRECT = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-latest-redirect.sql.gz")
WIKIPEDIA_CATEGORYLINKS = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-latest-categorylinks.sql.gz")
WIKIPEDIA_PAGEPROPS = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-latest-page_props.sql.gz")
WIKIPEDIA_PAGE = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-latest-page.sql.gz")
WIKIMAPPER = CollectionDataset(f"{CONFIG.remote_prefix}index_enwiki-latest.db")
QRANK = CollectionDataset(f"{CONFIG.remote_prefix}qrank.csv")



def wget_for_wikidata(type: str):
    return f"https://dumps.wikimedia.org/wikidatawiki/entities/latest-{type}.nt.bz2"


with DAG(
    "download-wikidata",
    default_args={
        "cwd": CONFIG.local_prefix,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    description="Tasks related to downloading Wikidata source files",
    schedule=CONFIG.run_interval,
    catchup=False,
    tags=["download", "collection-templates"],
) as dag:

    dag.doc_md = """
    Downloading of Wikidata files.
    """  

    pagelinks_task = BashOperator(
        outlets=[WIKIDATA_TRUTHY],
        task_id="download-truthy",
        bash_command=f"wget {wget_for_wikidata('truthy')} -O {WIKIDATA_TRUTHY.local_name()}",
        start_date=CONFIG.start_date,
    )

    pagelinks_task.doc_md = dedent(
        """\
    #### Task Documentation
    Download the latest **thruthy** statements from Wikidata.
    """
    )


with DAG(
    "filter-wikidata",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    description="Tasks related to processing of source Wikidata files",
    schedule=[WIKIDATA_TRUTHY],
    catchup=False,
    tags=["filter", "collection-templates"],
) as dag:

    regex = "'^(<https://en\.wikipedia\.org/wiki/|<http://www\.wikidata\.org/entity/).*((<http://www\.wikidata\.org/prop/direct/P18>|<http://www\.wikidata\.org/prop/direct/P1753>|<http://www\.wikidata\.org/prop/direct/P31>|<http://schema\.org/about>|<http://www\.wikidata\.org/prop/direct/P1754>|<http://www\.wikidata\.org/prop/direct/P4224>|<http://www\.wikidata\.org/prop/direct/P948>|<http://www\.wikidata\.org/prop/direct/P279>|<http://www\.wikidata\.org/prop/direct/P360>|<http://www\.w3\.org/2002/07/owl\#sameAs>)|((<http://schema\.org/description>|<http://schema\.org/name>|<http://www\.w3\.org/2000/01/rdf\-schema\#label>).*@en .$$))'"
    filter_wikidata_task = BashOperator(
        outlets=[WIKIDATA_FILTERED],
        task_id="grep-wikidata",
        cwd=f"{CONFIG.local_prefix}",
        bash_command=f"lbzip2 -d {WIKIDATA_TRUTHY.local_name()} --stdout | grep -E {regex} | lbzip2 -c > {WIKIDATA_FILTERED.local_name()}",
        start_date=CONFIG.start_date,
    )

    filter_wikidata_task.doc_md = dedent(
        """\
    #### Task Documentation
    This task filters the predicates in the Wikipedia dump to include 
    only a subset of predicates and and a subset of entities (only wikidata and English wikipedia).
    """
    )

    dag.doc_md = """
    Filtering Wikidata for entries including only specific subjects and predicates.
    """  

def wget_for_wikipedia(dataset):
    return f"https://dumps.wikimedia.org/enwiki/latest/{dataset.name()} -O {dataset.local_name()}"

with DAG(
    "download-wikipedia",
    default_args={
        "cwd": CONFIG.local_prefix,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 5,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related to downloading Wikipedia source files",
    schedule=CONFIG.run_interval,
    catchup=False,
    tags=["download", "collection-templates"],
    max_active_tasks=2,
) as dag:

    dag.doc_md = """
    Downloading of Wikipedia dump files
    """  

    pagelinks_task = BashOperator(
        outlets=[WIKIPEDIA_PAGELINKS],
        task_id="download-pagelinks",
        bash_command=f"wget {wget_for_wikipedia(WIKIPEDIA_PAGELINKS)}",
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
        bash_command=f"wget {wget_for_wikipedia(WIKIPEDIA_REDIRECT)}",
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
        bash_command=f"wget {wget_for_wikipedia(WIKIPEDIA_CATEGORYLINKS)}",
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
        bash_command=f"wget {wget_for_wikipedia(WIKIPEDIA_PAGEPROPS)}",
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
        bash_command=f"wget {wget_for_wikipedia(WIKIPEDIA_PAGE)}",
    )

    page_task.doc_md = dedent(
        """\
    #### Task Documentation
    Download the latest **page** file.
    """
    )


with DAG(
    "wikimapper",
    default_args={
        "cwd": CONFIG.local_prefix,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related to running the wikimapper service",
    catchup=False,
    tags=["db", "collection-templates"],
    schedule=[WIKIPEDIA_REDIRECT, WIKIPEDIA_PAGE, WIKIPEDIA_PAGEPROPS],
) as dag:
    dag.doc_md = """
    Loading of Wikipedia dump files to wikimapper database
    """  
    wikimapper_task = BashOperator(
        outlets=[WIKIMAPPER],
        task_id="load-redirect",
        bash_command=f"wikimapper create enwiki-latest --dumpdir {CONFIG.local_prefix} --target {WIKIMAPPER.name()}",
    )

    wikimapper_task.doc_md = dedent(
        """\
    #### Task Documentation
    Load the redirects to the wikimapper database.
    """
    )

with DAG(
    "qrank",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related downloading of rank files.",
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["rank"],
    schedule=CONFIG.run_interval,
) as dag:
    qrank_task = BashOperator(
        outlets=[QRANK],
        task_id="download-qrank",
        bash_command=f"wget -O - https://qrank.wmcloud.org/download/qrank.csv.gz | gunzip -c > {QRANK.local_name()}",
    )
