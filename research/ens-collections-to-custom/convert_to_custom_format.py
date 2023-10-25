import subprocess
import csv
import jsonlines
import json
import shutil
from pathlib import Path
from argparse import ArgumentParser



ens_collections_repo_url = 'https://github.com/Zimtente/ens-collections.git'
data_path = Path.cwd() / 'data'
repo_path = data_path / 'ens-collections'
ens_collections_metadata_path = repo_path / 'ens-collections.json'


def clone_repo():
    if not ens_collections_metadata_path.is_file():
        subprocess.run(["git", "clone", ens_collections_repo_url, repo_path])


def remove_repo():
    shutil.rmtree(repo_path)


def read_metadata() -> dict:
    with open(ens_collections_metadata_path, 'r') as f:
        data = json.load(f)
    return data


def save_custom_collections(output_path: Path, custom_collections: list[dict]):
    with jsonlines.open(output_path, 'w') as writer:
        for c in custom_collections:
            writer.write(c)


def extract_names(csv_filename: str) -> list[dict]:
    names = []

    csv_path = repo_path / 'collections' / csv_filename
    with open(csv_path, newline='', encoding='utf-8') as f:
        for row in csv.reader(f, delimiter=','):
            # FIXME: not always are the columns in correct order
            names.append(
                {
                    "normalized_name": row[0],
                    # "tokenized_name": row[0]  # todo: row[0] for collections with non-tokenizable names? (like '0x17')
                }
            )
    return names

def transform_collections(metadata: dict) -> list[dict]:
    metadata_per_collection = metadata['collections']

    transformed_collections = []

    # assert unique slugs
    slugs = [r["slug"] for r in metadata_per_collection]
    assert len(slugs) == len(set(slugs)), f'slug ids are not unique! [ {len(set(slugs))} / {len(slugs)} ]'
    # FIXME: they are not !!! :)  [ 138 / 140 ]

    # assert single csv file for each collection
    assert all(len(r["csv"]) == 1 for r in metadata_per_collection), 'some collections have more than 1 csv file!'


    for c_meta_record in metadata_per_collection:
        transformed_collections.append(
            {
                "commands": {"sort_names": "none"},  # todo: is none ok?
                "data": {
                    "collection_id": c_meta_record["slug"],
                    "collection_name": c_meta_record["name"],
                    "collection_description": c_meta_record["description"],
                    "collection_keywords": ["ens-clubs"],
                    # "avatar_emoji": None,
                    "names": extract_names(csv_filename=c_meta_record["csv"][0])
                }
            }
        )

    return transformed_collections





if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-o', '--output', help='output filename', default='custom-ens-collections')
    parser.add_argument('--save_repo', help='if set, the cloned repo will not be removed',
                        action='store_true', default=False)
    args = parser.parse_args()
    output_path = Path(args.output) if args.output.endswith('.jsonl') else Path(args.output + '.jsonl')
    output_path = output_path.resolve()
    save_repo = args.save_repo

    clone_repo()
    meta = read_metadata()

    transformed = transform_collections(meta)

    save_custom_collections(output_path, transformed)
    if not save_repo:
        remove_repo()

# python research/ens-collections-to-custom/convert_to_custom_format.py --save_repo
