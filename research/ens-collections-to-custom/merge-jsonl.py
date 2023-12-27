from argparse import ArgumentParser
import jsonlines


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--input', type=str, nargs='+', required=True, help='path to the input JSONL file')
    parser.add_argument('--output', type=str, required=True, help='path to the output JSONL file')
    args = parser.parse_args()

    with jsonlines.open(args.output, 'w') as writer:
        for input_path in args.input:
            with jsonlines.open(input_path, 'r') as reader:
                for obj in reader:
                    writer.write(obj)
