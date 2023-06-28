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

    if not df['id'].is_unique:
        print("Warning: duplicate IDs found. Duplicates will be dropped.")
        df.drop_duplicates(subset='id', keep='first', inplace=True)
    
    print(f'Number of unique collections: {len(df)}')  

    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    
    # save other collections
    others = df[['id', 'name']]
    with open(output_dir / 'others.json', 'w') as f:
        json.dump({'rows': others.to_dict(orient='records')}, f, indent=4)
    
    # save writer's block collections with score >= min_wblock_score
    wblock = df[df[score_column] >= min_wblock_score][['id', 'name']]
    with open(output_dir / 'wblock.json', 'w') as f:
        json.dump({'rows': wblock.to_dict(orient='records')}, f, indent=4)




if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Create collection sets: 'writers_block.json' and 'other_collections.json', "
                    "with schema (id, title), from specified path containing .tsv collections."
    )
    parser.add_argument('--default_score', 
                        help='default score for .tsv files with no score column', default=6, type=int)
    parser.add_argument('--min_othercollections_score', 
                        help='minimum score for other collections', default=3, type=int)
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
