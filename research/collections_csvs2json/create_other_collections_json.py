import argparse
import json
import pandas as pd
from pathlib import Path


def create_collections_sets(
        input_dir: Path, output_dir: Path, 
        default_score: int, min_other_score: int, min_wblock_score: int,
        score_column = 'TC'
        ):
    
    df = pd.DataFrame()
    for file in input_dir.iterdir():
        if file.suffix == '.tsv':
            temp_df = pd.read_csv(file, sep='\t')
            if score_column in temp_df.columns:
                temp_df = temp_df[temp_df[score_column].notna()]
                temp_df = temp_df[temp_df[score_column] >= min_other_score]
            else:
                temp_df[score_column] = default_score
            df = pd.concat([df, temp_df], ignore_index=True)

    print(f'Number of total collections: {len(df)}')
    print(f'Number of collections with score >= {min_wblock_score}: {len(df[df[score_column] >= min_wblock_score])}')

    if not df['id'].is_unique:
        print("Warning: duplicate IDs found. Duplicates will be dropped.")
        df = df.sort_values(score_column, ascending=False).drop_duplicates('id', keep='first')
    
    print(f'Number of unique collections: {len(df)}')
    print(f'Number of unique collections with score >= {min_wblock_score}: {len(df[df[score_column] >= min_wblock_score])}')

    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    
    # save other collections
    others_filename = 'other_collections.json'
    others = df[['id', 'name']]
    print(f"Saving {len(others)} other collections to {others_filename} ...")
    with open(output_dir / others_filename, 'w') as f:
        json.dump(others.to_dict(orient='records'), f, indent=4)
    
    # save writer's block collections with score >= min_wblock_score
    wblock_filename = 'wblock_collections.json'
    wblock = df[df[score_column] >= min_wblock_score][['id', 'name']]
    print(f"Saving {len(wblock)} writer's block collections to {wblock_filename} ...")
    with open(output_dir / wblock_filename, 'w') as f:
        json.dump(wblock.to_dict(orient='records'), f, indent=4)




if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Create collection sets: 'writers_block.json' and 'other_collections.json', "
                    "with schema (id, title), from specified path containing .tsv collections."
    )
    parser.add_argument('--default_score', 
                        help='default score for .tsv files with no score column', default=6, type=int)
    parser.add_argument('--min_othercollections_score', 
                        help='minimum score for other collections', default=4, type=int)
    parser.add_argument('--min_writersblock_score', 
                        help='minimum score for writers block collections', default=6, type=int)
    parser.add_argument('-i', '--input', 
                        help='input directory path containing .tsv files', default='./collections_tsv_data', type=str)
    parser.add_argument('-o', '--output', 
                        help='output directory path', default='./', type=str)
    args = parser.parse_args()
    
    create_collections_sets(
        Path(args.input), Path(args.output), 
        args.default_score, args.min_othercollections_score, args.min_writersblock_score
        )
