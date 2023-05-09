.PHONY: all

#TODO everything should be unquoted because some characters are not escaped properly, e.g. comma

filter: data/latest-all.nt.bz2.filtered.bz2 data/stats_predicates.txt data/stats_instance_of.txt

data/latest-all.nt.bz2:
	time wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.nt.bz2

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
	# output:
	#  {
	#    "item": "Q100088400",
	#    "type": [
	#      "Q5"
	#    ],
	#    "article": "Category:Writers_from_Z%C3%BCrich"
	#  },	

data/lists2.json: dictrocks dictrocks_rev
	time python scripts/create_lists.py $@ --mode list
	# output:
	#   {
	#    "item": "Q100673110",
	#    "type": [
	#      "Q11424"
	#    ],
	#    "article": "List_of_Walt_Disney_Studios_films_(2000%E2%80%932009)"
	#  },

########################  DOWNLOADING MEMBERS  ########################
download_members: download_list_members download_category_members

############## LIST MEMBERS ##############
# FIXME currently parser expects the gz to have date in it (think about PR to fix that)
data/enwiki-20230401-pagelinks.sql.gz:
	time wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pagelinks.sql.gz -O $@

data/allowed-lists.txt: data/lists2.json data/index_enwiki-latest.db
	time python scripts/extract_allowed_lists.py $^ $@
	# output is a list of page ids:
	# 23455140

data/enwiki-pagelinks.csv: data/enwiki-20230401-pagelinks.sql.gz data/allowed-lists.txt
	time python scripts/parse_wiki_dump.py $< $@ --mode list --allowed_values data/allowed-lists.txt
	# 126m, output:
	# 23455140,1.FC_Nürnberg
    # 33030326,1.FC_Nürnberg

data/mapped-lists.csv: data/enwiki-pagelinks.csv data/index_enwiki-latest.db
	time python scripts/map_to_wikidata_ids_and_titles.py $^ $@ --mode list
	# output:
	# Q6620950,1.FC_Nürnberg
	# Q6589143,1.FC_Nürnberg
	# List_of_footballers_killed_during_World_War_II,1.FC_Nürnberg
  	# List_of_Malaysian_football_transfers_2012,1.FC_Nürnberg
  	# Swedish_women's_football_clubs_in_international_competitions,1.FFC_Frankfurt

data/sorted-lists.csv: data/mapped-lists.csv
	(head -n 1 $< && tail -n +2 $< | LC_ALL=C sort) > $@
	# 1954_FIFA_World_Cup_squads,1._FC_Nürnberg

data/list_links2.jsonl: data/sorted-lists.csv data/lists2.json
	time python scripts/reformat_csv_to_json.py $< $@ --list_of_collections data/lists2.json --mode list
	# output
	#     {"item": "Q1000775", "type": ["Q11446"], "article": "SMS_W%C3%BCrttemberg", "members": ["SMS Württemberg (1878)", "Bayern-class battleship", "Kaiserliche Marine", "SMS Württemberg",  "SMS Württemberg (1917)", "Sachsen-class ironclad", "WikiProject Ships/Guidelines"]}
    # old:{"item": "Q1000775", "type": ["Q11446"], "article": "SMS_W%C3%BCrttemberg", "members": ["SMS Württemberg (1878)", "Bayern-class battleship", "Kaiserliche Marine", "Sachsen-class ironclad", "SMS Württemberg (1917)"]}
download_list_members: data/list_links2.jsonl

############## CATEGORY MEMBERS ##############
# FIXME currently parser expects the gz to have date in it (think about PR to fix that)
data/enwiki-20230401-categorylinks.sql.gz:
	time wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-categorylinks.sql.gz -O $@
	# 143237,'Writers_from_Zürich',...

data/allowed-categories.txt: data/categories2.json
	time python scripts/extract_allowed_categories.py $< $@
	# output is a list of category titles:
	# Category:Writers_from_Z%C3%BCrich

data/enwiki-categories.csv: data/enwiki-20230401-categorylinks.sql.gz data/allowed-categories.txt
	time python scripts/parse_wiki_dump.py $< $@ --mode category --allowed_values data/allowed-categories.txt
	# 33m: output
	# 143237,Writers_from_Zürich

data/mapped-categories.csv: data/enwiki-categories.csv data/index_enwiki-latest.db data/categories2.json
	time python scripts/map_to_wikidata_ids_and_titles.py $< data/index_enwiki-latest.db $@ --mode category --categories data/categories2.json
	# 3m, output:
	# Q8879623,Park_Güell
	# Writers_from_Zürich,Johann_Georg_Baiter
	# Antoni_Gaudí_buildings,Park_Güell

data/sorted-categories.csv: data/mapped-categories.csv
	(head -n 1 $< && tail -n +2 $< | LC_ALL=C sort) > $@

data/category_members2.jsonl: data/sorted-categories.csv data/categories2.json
	time python scripts/reformat_csv_to_json.py $< $@ --list_of_collections data/categories2.json --mode category
	# output:
	# {"item": "Q100088400", "type": ["Q5"], "article": "Category:Writers_from_Z%C3%BCrich", "members": ["Alain de Botton", "Annemarie Schwarzenbach", "Arnold Kübler", "Bernhard Diebold", "Bruno Barbatti", "Carl Seelig", "Charles Lewinsky", "Conrad Ferdinand Meyer", "Egon von Vietinghoff", "Elisabeth Joris", "Esther Dyson", "Fleur Jaeggy", "Gerold Meyer von Knonau (1804–1858)", "Gottfried Keller", "Gotthard Jedlicka", "Hans-Ulrich Indermaur", "Hugo Loetscher", "Ilma Rakusa", "Johann Caspar Scheuchzer", "Johann Georg Baiter", "Johann Jakob Breitinger", "Johann Jakob Hottinger (historian)", "Johann Kaspar Lavater", "Jürg Schubiger", "Ludwig Hirzel (historian)", "Mariella Mehr", "Markus Hediger", "Max Frisch", "Max Rychner", "Moustafa Bayoumi", "Olga Plümacher", "Peter Zeindler", "Robert Faesi", "Roger Sablonier", "Stefan Maechler", "Taya Zinkin", "Verena Conzett", "Werner Vordtriede", "Wilhelm Wartmann"]}

download_category_members: data/category_members2.jsonl
	
########################  WIKIMAPPER SETUP  ########################
wikimapper: data/index_enwiki-latest.db

wikimapper_download:
	time wikimapper download enwiki-latest --dir data

data/index_enwiki-latest.db: wikimapper_download
	time wikimapper create enwiki-latest --dumpdir data --target $@
	
# wikimapper stores: wikipedia_id, wikipedia_title, wikidata_id
# also redirects, for which wikidata_id overriden by target aricle

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