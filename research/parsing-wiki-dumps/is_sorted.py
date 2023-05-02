from argparse import ArgumentParser
import csv


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help='csv input filepath')
    args = parser.parse_args()

    with open(args.input, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        header = next(reader)  # skipping header

        prev_line = next(reader)
        for line in reader:
            if int(line[0]) < int(prev_line[0]):
                print(prev_line)
                print(line)
                print('false')
                exit(1)

            prev_line = line
