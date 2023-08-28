from argparse import ArgumentParser
from collections import defaultdict
import json


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='input TXT file with collection names')
    parser.add_argument('output', help='output JSON file')
    parser.add_argument('--max-tokens', default=2, type=int, help='maximum number of tokens in prefix')
    parser.add_argument('--min-count', default=2, type=int, help='minimum number of occurrences')
    parser.add_argument('--examples', default=5, type=int, help='number of examples to show')
    args = parser.parse_args()

    with open(args.input, 'r', encoding='utf-8') as f:
        collection_names = [line.strip() for line in f]

    prefix2count = defaultdict(int)
    prefix2examples = defaultdict(list)
    for collection_name in collection_names:
        prefix_tokens = collection_name.split()[:args.max_tokens]

        for i in range(len(prefix_tokens)):
            tokens = prefix_tokens[:i+1]
            prefix = ' '.join(tokens)
            prefix2count[prefix] += 1
            prefix2examples[prefix].append(collection_name)

    prefixes = []
    for prefix, count in prefix2count.items():
        if count >= args.min_count:
            prefixes.append({
                'prefix': prefix,
                'count': count,
                'examples': prefix2examples[prefix][:args.examples]
            })

    prefixes = sorted(prefixes, key=lambda x: x['count'], reverse=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(prefixes, f, indent=2, ensure_ascii=False)
