.PHONY: all

filter: data/latest-all.nt.bz2.filtered.bz2 data/stats_predicates.txt data/stats_instance_of.txt

data/latest-all.nt.bz2.filtered.bz2: data/latest-all.nt.bz2
	time pv $< | bzcat | grep -E '^(<https://en\.wikipedia\.org/wiki/|<http://www\.wikidata\.org/entity/).*((<http://www\.wikidata\.org/prop/direct/P18>|<http://www\.wikidata\.org/prop/direct/P1753>|<http://www\.wikidata\.org/prop/direct/P31>|<http://schema\.org/about>|<http://www\.wikidata\.org/prop/direct/P1754>|<http://www\.wikidata\.org/prop/direct/P4224>|<http://www\.wikidata\.org/prop/direct/P948>|<http://www\.wikidata\.org/prop/direct/P279>|<http://www\.wikidata\.org/prop/direct/P360>|<http://www\.w3\.org/2002/07/owl\#sameAs>)|((<http://schema\.org/description>|<http://schema\.org/name>|<http://www\.w3\.org/2000/01/rdf\-schema\#label>).*@en .$$))' | pbzip2 -c > $@

data/stats_predicates.txt: data/latest-all.nt.bz2.filtered.bz2
	pv $< | pbzip2 -d -c | cut -d ' ' -f 2 | sort | uniq -c > $@
	cat $@

data/stats_instance_of.txt: data/latest-all.nt.bz2.filtered.bz2
	pv $< | pbzip2 -d -c | grep "<http://www\.wikidata\.org/prop/direct/P31>" | cut -d ' ' -f 3 | sort | uniq -c | sort -nr > $@
	head -n 20 $@

dictrocks: data/latest-all.nt.bz2.filtered.bz2 # 1.5h
	time python scripts/create_kv.py $<

dictrocks_rev:
	time python scripts/reverse_rocksdict.py data/db1.rocks/ data/db1_rev.rocks/

########################  PREPARING VALID LISTS AND CATEGORIES  ########################
prepare_lists_and_categories: data/categories2.json data/lists2.json

data/categories2.json: dictrocks dictrocks_rev
	time python scripts/create_lists.py $@ --mode category
	
data/lists2.json: dictrocks dictrocks_rev
	time python scripts/create_lists.py $@ --mode list


########################  DOWNLOADING MEMBERS  ########################
download_members: download_list_members download_category_members

############## LIST MEMBERS ##############
data/enwiki-20230401-pagelinks.sql.gz:
	# fixme

data/enwiki-20230401-pagelinks.sql: data/enwiki-20230401-pagelinks.sql.gz
	# fixme

data/allowed-lists.txt: data/lists2.json data/index_enwiki-latest.db
	time python scripts/extract_allowed_lists.py $< data/index_enwiki-latest.db data/allowed-lists.txt

data/enwiki-pagelinks.csv: data/enwiki-20230401-pagelinks.sql data/allowed-lists.txt
	time python scripts/parse_wiki_dump.py $< data/enwiki-pagelinks.csv --mode list --allowed_values data/allowed-lists.txt

data/mapped-lists.csv: data/enwiki-pagelinks.csv data/index_enwiki-latest.db
	time python scripts/map_to_wikidata_ids_and_titles.py $< data/index_enwiki-latest.db data/mapped-lists.csv --mode list

data/sorted-lists.csv: data/mapped-lists.csv
	(head -n 1 $< && tail -n +2 $< | sort) > sorted-lists.csv

data/list_links2.jsonl: data/sorted-lists.csv data/lists2.json
	time python scripts/reformat_csv_to_json.py $< data/list_links2.jsonl --list_of_collections data/lists2.json --mode list

download_list_members: data/list_links2.jsonl

############## CATEGORY MEMBERS ##############
data/enwiki-20230401-categorylinks.sql.gz:
	# fixme

data/enwiki-20230401-categorylinks.sql: data/enwiki-20230401-categorylinks.sql.gz
	# fixme

data/allowed-categories.txt: data/categories2.json
	time python extract_allowed_categories.py $< data/allowed-categories.txt

data/enwiki-categories.csv: data/enwiki-20230401-categorylinks.sql data/allowed-categories.txt
	time python scripts/parse_wiki_dump.py $< data/enwiki-categories.csv --mode category --allowed_values data/allowed-categories.txt

data/mapped-categories.csv: data/enwiki-categories.csv data/index_enwiki-latest.db data/categories2.json
	time python scripts/map_to_wikidata_ids_and_titles.py $< data/index_enwiki-latest.db data/mapped-categories.csv --mode category --categories data/categories2.json

data/sorted-categories.csv: data/mapped-categories.csv
	(head -n 1 $< && tail -n +2 $< | sort) > sorted-categories.csv

data/category_members2.jsonl: data/sorted-categories.csv data/categories2.json
	time python scripts/reformat_csv_to_json.py $< data/category_members2.jsonl --list_of_collections data/categories2.json --mode category

download_category_members: data/category_members2.jsonl
	
########################  WIKIMAPPER SETUP  ########################
wikimapper: data/index_enwiki-latest.db

wikimapper_download:
	time wikimapper download enwiki-latest --dir data

data/index_enwiki-latest.db: wikimapper_download
	time wikimapper create enwiki-latest --dumpdir data --target $@
	
########################  QRANK  ########################
qrank: data/qrank.csv

data/qrank.csv:
	time wget -O - https://qrank.wmcloud.org/download/qrank.csv.gz | gunzip -c > $@

########################  ???  ########################

# filter members of lists and categories
# Wikidata redirects

#TODO cache force normalize

cache_interesting_score: cache_interesting_score_lists cache_interesting_score_categories

cache_interesting_score_lists: data/validated_list_links.jsonl
	time python scripts/cache_interesting_score.py $< -n 111000

cache_interesting_score_categories: data/validated_category_members.jsonl
	time python scripts/cache_interesting_score.py $< -n 460000

data/validated_list_links2.jsonl: data/list_links2.jsonl
	time python3 scripts/filter_articles2.py $< $@ -n 111000
# 29:08<00:33, 62.30it/s
#Members 7052484 valid, 22489645 invalid
#No parent 1308946
#No parent 642624
#but should be Members 7.057.739 valid, 25.985.431 invalid
	
data/validated_category_members2.jsonl: data/category_members2.jsonl
	time python3 scripts/filter_articles2.py $< $@ -n 460000
# 461543it [22:38, 339.84it/s]
#Members 20456859 valid, 8641896 invalid
#No parent 1358593
#should be Members 21.294.548 valid, 7.888.585 invalid

data/category_members_all_info.jsonl: data/validated_category_members2.jsonl
	time python3 scripts/prepare_members_names.py $< data/qrank.csv $@ -n 460000
	
data/list_links_all_info.jsonl: data/validated_list_links2.jsonl
	time python3 scripts/prepare_members_names.py $< data/qrank.csv $@ -n 111000
	

	#time python3 scripts/prepare_collections2.py data/list_links_all_info.jsonl data/list_links_final.jsonl -n 111000

	#time python3 scripts/prepare_collections2.py data/category_members_all_info.jsonl data/category_members_final.jsonl -n 460000

data/merged.jsonl: data/list_links_all_info.jsonl data/category_members_all_info.jsonl
	time python scripts/merge_lists_and_categories.py data/list_links_all_info.jsonl data/category_members_all_info.jsonl data/merged.jsonl

	# All collections: 570487
	#Lists: 108944, Categories: 461543, Written 511932
	#Merged by type 6996 categories into lists
	#Merged by name 6720 categories into lists
	#Filtered by type: 44096
	#Filtered by prefix: 743


	#All collections: 570487
	#Lists: 108944, Categories: 461543, Written 503427
	#Merged by type 6920 categories into lists
	#Merged by name 6712 categories into lists
	#Filtered by type: 44096
	#Filtered by prefix: 743
	#Filtered by by: 8589

data/merged_filtered.jsonl: data/merged.jsonl
	python scripts/merge_collections_ending_with_letters.py data/merged.jsonl data/merged_filtered.jsonl -n 503427
	#Matches: 3554
	#Merged: 3462

data/merged_filtered_dup.jsonl: data/merged_filtered.jsonl
	time python scripts/filter_duplicates.py data/merged_filtered.jsonl data/merged_filtered_dup.jsonl -n 500139
	# Merged: 261

data/merged_final.jsonl: data/merged_filtered_dup.jsonl
	time python3 scripts/prepare_collections2.py data/merged_filtered_dup.jsonl data/merged_final.jsonl -n 500008
	
# finally 419030