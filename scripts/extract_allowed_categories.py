from argparse import ArgumentParser
import json


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('lists', help='filepath to the JSON file')
    parser.add_argument('output', help='path for the output text file containing category titles')
    args = parser.parse_args()

    with open(args.lists, 'r', encoding='utf-8') as f:
        categories = json.load(f)

    titles = [category['article'] for category in categories]

    with open(args.output, 'w', encoding='utf-8') as f:
        f.write('\n'.join(titles) + '\n')
