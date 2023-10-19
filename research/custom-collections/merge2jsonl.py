from argparse import ArgumentParser
import jsonlines
import json


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--input', type=str, nargs='+', required=True, help='Paths to the input JSON files')
    parser.add_argument('--output', type=str, required=True, help='Path to the output JSONL file')
    args = parser.parse_args()

    with jsonlines.open(args.output, 'w') as writer:
        for input_path in args.input:
            with open(input_path, 'r', encoding='utf-8') as f:
                record = json.load(f)
                writer.write(record)
