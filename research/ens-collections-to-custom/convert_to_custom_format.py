import subprocess
import csv
import json
import shutil
from pathlib import Path
from argparse import ArgumentParser



ens_collections_repo_url = 'https://github.com/Zimtente/ens-collections.git'
data_path = Path.cwd() / 'data'
repo_path = data_path / 'ens-collections'
ens_collections_metadata_path = repo_path / 'ens-collections.json'
raw_collections_path = repo_path / 'collections'
whitelist_path = Path(__file__).resolve().parent / 'ens_clubs_whitelist_slugs.csv'


def clone_repo():
    if not ens_collections_metadata_path.is_file():
        subprocess.run(["git", "clone", ens_collections_repo_url, repo_path])


def remove_repo():
    shutil.rmtree(repo_path)


def rename_files_to_lowercase(dir_path: Path):
    assert dir_path.is_dir(), f"{dir_path} is not a directory!"
    print('\nRenaming csv files...')
    for file_path in dir_path.iterdir():
        if file_path.is_file():
            new_name = file_path.name.lower()
            new_file_path = file_path.parent / new_name
            if file_path != new_file_path:
                file_path.rename(new_file_path)
                print(f"\t{file_path.name} -> {new_file_path.name}")
    print()


def read_metadata() -> dict:
    with open(ens_collections_metadata_path, 'r') as f:
        data = json.load(f)
    return data


def save_custom_collections(custom_collections: list[dict], json_output_path: Path):
    json_output_path.mkdir(exist_ok=True)

    print(f'\nSaving transformed collections as separates jsons to "{json_output_path}"')
    for collection_record in custom_collections:
        filename = f'{collection_record["data"]["collection_id"]}.json'
        with open(json_output_path / filename, 'w', encoding='utf-8') as f:
            json.dump(collection_record, f, ensure_ascii=False, indent=4)


def filter_whitelist(metadata_list: list[dict]) -> list[dict]:
    with open(whitelist_path, 'r') as f:
        whitelist_slugs = [row[0] for row in csv.reader(f)]
    return list(filter(lambda r: r['slug'] in whitelist_slugs, metadata_list))


def remove_duplicates_from_metadata(metadata_list: list[dict]) -> list[dict]:
    slugs = [r["slug"] for r in metadata_list]
    slugs_set = set(slugs)
    for s in slugs_set:
        slugs.remove(s)

    for duplicate_s in slugs:
        # remove first record with this slug
        to_remove_idx = None
        for i, r in enumerate(metadata_list):
            if r['slug'] == duplicate_s:
                to_remove_idx = i
        metadata_list.pop(to_remove_idx)

    return metadata_list


def extract_names(csv_filename: str) -> list[dict]:
    names = []

    csv_path = raw_collections_path / csv_filename
    with open(csv_path, newline='', encoding='utf-8') as f:
        for row in csv.reader(f, delimiter=','):
            names.append(
                {
                    "normalized_label": row[0],
                    # "tokenized_label": row[0]  # no tokenization here
                }
            )
    return names


def transform_collections(metadata: dict) -> list[dict]:
    metadata_per_collection = metadata['collections']

    metadata_per_collection = filter_whitelist(metadata_per_collection)

    metadata_per_collection = remove_duplicates_from_metadata(metadata_per_collection)

    # assert unique slugs
    slugs = [r["slug"] for r in metadata_per_collection]
    assert len(slugs) == len(set(slugs)), f'slug ids are not unique! [ {len(set(slugs))} / {len(slugs)} ]'

    # assert single csv file for each collection
    assert all(len(r["csv"]) == 1 for r in metadata_per_collection), 'some collections have more than 1 csv file!'

    # assert all csv file names in metadata are lowercase
    assert all(r["csv"][0].islower() for r in metadata_per_collection), 'not all csv file names are lowercase!'

    # non-trivial tokenization for collections below:
    #
    # 365-club.csv : april23th -> april, 25th
    # country-leaders.csv : no tokenization (?) (names and surnames)
    # english-animals.csv : no tokenization (?) (latin names of species)
    # ens-date-club.csv : 4jan -> 4, jan
    # ens-full-date-club.csv : 10december -> 10, december
    # all collections with emojis only : treat as one token (?)
    # flagcountry-club.csv : 🇦🇫afghanistan -> 🇦🇫, afghanistan
    # got-houses-club.csv : houseblackberry -> houseblackfyre -> house, blackfyre
    # harry-potter.csv : no tokenization (?) (names and surnames)
    # kanye-ens-club.csv : no tokenization (?) (mix of unigrams and n>1grams)
    # marvel-club.csv : no tokenization (?) (mix of names)
    # naruto-names.csv : no tokenization (?) (names and surnames)
    # playstation-console-series.csvc : no tokenization (?) (playstation4slim etc.)
    # pre-punk-1k.csv, pre-punk-10k.csv, pre-punk-club.csv, pre-punk-spanish.csv : no tokenization (?) (mixed)
    # psalms-club.csv : psalm100 -> psalm, 100
    # skateboard-tricks.csv : no tokenization (?) (mix of trick and numbers)
    # spanish-animals.csv : no tokenization (?) (latin/english names of species)
    # the-cents-club.csv : 17cents -> 17, cents
    # tolkien.csv : no tokenization (?) (names and surnames)
    # tolkien.csv : no tokenization (?) (names and surnames)
    # un-countries.csv : no tokenization (?) (some countries are multi-words like dominicanrepublic)
    # us-999-club.csv : 🇺🇸998 -> 🇺🇸, 998

    # not doing any tokenization here for now

    transformed_collections = []

    for c_meta_record in metadata_per_collection:
        transformed_collections.append(
            {
                "commands": {
                    "sort_names": "none",
                    "collection_rank": 300_000,
                    "member_rank": 2_000_000,
                },
                "data": {
                    "collection_id": c_meta_record["slug"],
                    "collection_name": c_meta_record["name"],
                    "collection_description": c_meta_record["description"],
                    "collection_keywords": ["ens-clubs"],
                    # "avatar_emoji": None,
                    "labels": extract_names(csv_filename=c_meta_record["csv"][0])
                }
            }
        )

    return transformed_collections


if __name__ == '__main__':
    parser = ArgumentParser(
        description="Download ens clubs from repo and save each of them to a json file using custom format."
    )
    parser.add_argument('-o', '--output', help='output directory', default='custom-ens-clubs')
    parser.add_argument('--save_repo', help='if set, the cloned repo will not be removed',
                        action='store_true', default=False)
    args = parser.parse_args()
    output_path = Path(__file__).resolve().parent / Path(args.output)
    save_repo = args.save_repo

    clone_repo()

    # required due to incorrect filenames in metadata (all are lowercase)
    rename_files_to_lowercase(raw_collections_path)

    meta = read_metadata()

    transformed = transform_collections(meta)

    save_custom_collections(transformed, json_output_path=output_path)

    if not save_repo:
        remove_repo()

# from repo root:
# python research/ens-collections-to-custom/convert_to_custom_format.py
