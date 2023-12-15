import argparse
import json
import jsonlines
from pathlib import Path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge json collection files into a single jsonl file."
    )
    parser.add_argument('--input', type=str,
                        help='path to the directory containing JSON files', default='custom-ens-clubs')
    parser.add_argument('--output', type=str,
                        help='path to the output JSONL file', default='data/ens_collections_fixed.jsonl')

    args = parser.parse_args()

    input_dir = Path(__file__).resolve().parent / Path(args.input)

    with jsonlines.open(args.output, mode='w') as writer:
        for file_path in input_dir.iterdir():
            if file_path.is_file() and file_path.suffix == '.json':
                with file_path.open('r') as json_file:
                    data = json.load(json_file)
                    writer.write(data)

# from repo root:
# python research/ens-collections-to-custom/jsons2jsonl.py --input ./custom-ens-clubs
