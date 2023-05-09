from argparse import ArgumentParser
import logging
from urllib.parse import unquote

from kwnlp_sql_parser import WikipediaSqlDump, WikipediaSqlCsvDialect


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = ArgumentParser(description='Parse wiki dump')
    parser.add_argument('input', help='sql dump filepath')
    parser.add_argument('output', help='output CSV file')
    parser.add_argument('--mode', default='category', choices=['category', 'list'], help='mode')
    parser.add_argument('--allowed_values', help='file with a list of wikipedia page ids which are lists')
    args = parser.parse_args()

    if args.mode == 'category':
        with open(args.allowed_values, 'r', encoding='utf-8') as f:
            category_titles = tuple([
                unquote(category_title.strip().removeprefix('Category:'))
                for category_title in f.read().strip('\n').split('\n')
            ])

        wsd = WikipediaSqlDump(
            args.input,
            keep_column_names=('cl_from', 'cl_to'),
            allowlists={'cl_to': category_titles}
        )
    elif args.mode == 'list':
        with open(args.allowed_values, 'r', encoding='utf-8') as f:
            page_ids = tuple([
                page_id.strip()
                for page_id in f.read().strip('\n').split('\n')
            ])

        wsd = WikipediaSqlDump(
            args.input,
            keep_column_names=('pl_from', 'pl_title'),
            allowlists={'pl_from': page_ids}
        )
    else:
        raise ValueError('either `categorylinks` or `pagelinks` flag must be set')

    wsd.to_csv(args.output)
