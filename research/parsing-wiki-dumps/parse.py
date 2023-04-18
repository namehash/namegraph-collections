from argparse import ArgumentParser
import logging

from kwnlp_sql_parser import WikipediaSqlDump, WikipediaSqlCsvDialect


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='sql dump filepath')
    parser.add_argument('-o', help='output CSV file')
    parser.add_argument('--categorylinks', action='store_true', help='flag for the categorylinks dump')
    parser.add_argument('--pagelinks', action='store_true', help='flag for the pagelinks dump')
    args = parser.parse_args()

    if args.categorylinks and args.pagelinks:
        raise ValueError('either `categorylinks` or `pagelinks` must be set, not both')

    if args.categorylinks:
        wsd = WikipediaSqlDump(args.input, keep_column_names=('cl_from', 'cl_to'))
    elif args.pagelinks:
        wsd = WikipediaSqlDump(args.input, keep_column_names=('pl_from', 'pl_title'))
    else:
        raise ValueError('either `categorylinks` or `pagelinks` flag must be set')

    wsd.to_csv(args.o)
