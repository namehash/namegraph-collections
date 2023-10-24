import subprocess
import jsonlines
import json
from pathlib import Path
from argparse import ArgumentParser



ens_collections_repo_url = 'https://github.com/Zimtente/ens-collections.git'
repo_path = Path.cwd() / 'ens-collections'
ens_collections_metadata_path = repo_path / 'ens-collections.json'


def clone_repo():
    if not ens_collections_metadata_path.is_file():
        subprocess.run(["git", "clone", ens_collections_repo_url, repo_path])


def read_metadata():
    with open(ens_collections_metadata_path, 'r') as f:
        data = json.load(f)
    return data




if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-o', '--output', help='output filename', default='custom-ens-collections')
    args = parser.parse_args()

    # .jsonl


