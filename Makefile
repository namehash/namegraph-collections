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
	python scripts/create_kv.py $<

dictrocks_rev:
	time python scripts/reverse_rocksdict.py data/db1.rocks/ data/db1_rev.rocks/

prepare_lists_and_categories: data/categories2.json data/lists2.json

data/categories2.json: dictrocks dictrocks_rev
	python scripts/create_lists.py $@ --mode category
	
data/lists2.json: dictrocks dictrocks_rev
	python scripts/create_lists.py $@ --mode list

download_members: download_list_members download_category_members

# the script is appending
download_list_members: data/lists2.json	
	time python scripts/download_category_members_and_links.py --mode list $< data/list_links2.jsonl

# the script is appending
download_category_members: data/categories2.json
	time python scripts/download_category_members_and_links.py --mode category $< data/category_members2.jsonl
	

wikimapper: data/index_enwiki-latest.db

wikimapper_download:
	wikimapper download enwiki-latest --dir data

data/index_enwiki-latest.db: wikimapper_download
	wikimapper create enwiki-latest --dumpdir data --target $@
	

qrank: data/qrank.csv

data/qrank.csv:
	wget -O - https://qrank.wmcloud.org/download/qrank.csv.gz | gunzip -c > $@

# filter members of lists and categories
# Wikidata redirects

#TODO cache force normalize

cache_interesting_score: cache_interesting_score_lists cache_interesting_score_categories

cache_interesting_score_lists: data/validated_list_links.jsonl
	time python scripts/cache_interesting_score.py $< -n 111000

cache_interesting_score_categories: data/validated_category_members.jsonl
	time python scripts/cache_interesting_score.py $< -n 460000

data/validated_list_links2.jsonl: data/list_links2.jsonl
	python3 scripts/filter_articles2.py $< $@ -n 111000
# 29:08<00:33, 62.30it/s
#Members 7052484 valid, 22489645 invalid
#No parent 1308946
#No parent 642624
#but should be Members 7.057.739 valid, 25.985.431 invalid
	
data/validated_category_members2.jsonl: data/category_members2.jsonl
	python3 scripts/filter_articles2.py $< $@ -n 460000
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

data/merged_final.jsonl: data/merged.jsonl
	time python3 scripts/prepare_collections2.py data/merged.jsonl data/merged_final.jsonl -n 511000