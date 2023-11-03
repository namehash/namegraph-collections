from argparse import ArgumentParser
from collections import defaultdict
import json


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='input TXT file with collection names')
    parser.add_argument('output', help='output JSON file')
    parser.add_argument('--max-tokens', default=2, type=int, help='maximum number of tokens in suffix')
    parser.add_argument('--min-count', default=2, type=int, help='minimum number of occurrences')
    parser.add_argument('--examples', default=5, type=int, help='number of examples to show')
    args = parser.parse_args()

    with open(args.input, 'r', encoding='utf-8') as f:
        collection_names = [line.strip() for line in f]

    suffix2count = defaultdict(int)
    suffix2examples = defaultdict(list)
    for collection_name in collection_names:
        suffix_tokens = collection_name.split()[-args.max_tokens:]

        for i in range(len(suffix_tokens)):
            tokens = suffix_tokens[i:]
            suffix = ' '.join(tokens)
            suffix2count[suffix] += 1
            suffix2examples[suffix].append(collection_name)

    suffixes = []
    for suffix, count in suffix2count.items():
        if count >= args.min_count:
            suffixes.append({
                'suffix': suffix,
                'count': count,
                'examples': suffix2examples[suffix][:args.examples]
            })

    suffixes = sorted(suffixes, key=lambda x: x['count'], reverse=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(suffixes, f, indent=2, ensure_ascii=False)
