from argparse import ArgumentParser
import jsonlines


if __name__ == '__main__':
    parser = ArgumentParser(description="Extract collection names from JSONL")
    parser.add_argument('input', help='input JSONL file')
    parser.add_argument('output', help='output TXT file')
    args = parser.parse_args()

    with jsonlines.open(args.input, 'r') as reader, open(args.output, 'w', encoding='utf-8') as writer:
        for collection in reader:
            collection_name = collection['data']['collection_name']
            writer.write(collection_name + '\n')
