pv /media/kwrobel/HDD_linux/wikidata/latest-all.nt.bz2 | bzcat | grep -E '^(<https://en\.wikipedia\.org/wiki/|<http://www\.wikidata\.org/entity/).*((<http://www\.wikidata\.org/prop/direct/P18>|<http://www\.wikidata\.org/prop/direct/P1753>|<http://www\.wikidata\.org/prop/direct/P31>|<http://schema\.org/about>|<http://www\.wikidata\.org/prop/direct/P1754>|<http://www\.wikidata\.org/prop/direct/P4224>|<http://www\.wikidata\.org/prop/direct/P948>|<http://www\.wikidata\.org/prop/direct/P279>|<http://www\.wikidata\.org/prop/direct/P360>)|((<http://schema\.org/description>|<http://schema\.org/name>|<http://www\.w3\.org/2000/01/rdf\-schema\#label>).*@en .$))' -> /media/kwrobel/HDD_linux/wikidata/latest-all.nt.bz2.filtered


wc -l latest-all.nt.bz2.filtered2
392602664 latest-all.nt.bz2.filtered2

pv latest-all.nt.bz2.filtered | cut -d ' ' -f 2 | sort | uniq -c
9309401 <http://schema.org/about>
84661447 <http://schema.org/description>
96340770 <http://schema.org/name>
      8 <http://wikiba.se/ontology#directClaim>
87031369 <http://www.w3.org/2000/01/rdf-schema#label>
  53703 <http://www.wikidata.org/prop/direct/P1753>
  53557 <http://www.wikidata.org/prop/direct/P1754>
4760137 <http://www.wikidata.org/prop/direct/P18>
3402348 <http://www.wikidata.org/prop/direct/P279>
105852824 <http://www.wikidata.org/prop/direct/P31>
 255450 <http://www.wikidata.org/prop/direct/P360>
 854851 <http://www.wikidata.org/prop/direct/P4224>
  26799 <http://www.wikidata.org/prop/direct/P948>

pv latest-all.nt.bz2.filtered | grep "http://wikiba\.se/ontology#directClaim"
<http://www.wikidata.org/entity/P4224> <http://wikiba.se/ontology#directClaim> <http://www.wikidata.org/prop/direct/P4224> .
<http://www.wikidata.org/entity/P279> <http://wikiba.se/ontology#directClaim> <http://www.wikidata.org/prop/direct/P279> .
<http://www.wikidata.org/entity/P1754> <http://wikiba.se/ontology#directClaim> <http://www.wikidata.org/prop/direct/P1754> .
<http://www.wikidata.org/entity/P31> <http://wikiba.se/ontology#directClaim> <http://www.wikidata.org/prop/direct/P31> .
<http://www.wikidata.org/entity/P948> <http://wikiba.se/ontology#directClaim> <http://www.wikidata.org/prop/direct/P948> .
<http://www.wikidata.org/entity/P1753> <http://wikiba.se/ontology#directClaim> <http://www.wikidata.org/prop/direct/P1753> .
<http://www.wikidata.org/entity/P360> <http://wikiba.se/ontology#directClaim> <http://www.wikidata.org/prop/direct/P360> .
<http://www.wikidata.org/entity/P18> <http://wikiba.se/ontology#directClaim> <http://www.wikidata.org/prop/direct/P18> .

pv latest-all.nt.bz2.filtered | grep "<http://www\.wikidata\.org/prop/direct/P31>" | cut -d ' ' -f 3 | sort | uniq -c



TODO:
remove instances of:
* scholarly article (Q13442814) 22,574,314 (31.5%)
* Wikimedia disambiguation page: 1,358,230 (1.9%)
* Wikimedia list article: 404,454 (0.6%)
* Wikimedia category: 1,358,230 (1.9%)

pv latest-all.nt.bz2.filtered | grep "<http://www\.wikidata\.org/prop/direct/P31>" | cut -d ' ' -f 3 | sort | uniq -c | sort -nr > instanceof_stats
38904978 <http://www.wikidata.org/entity/Q13442814> scholarly article
10594201 <http://www.wikidata.org/entity/Q5> human
5112506 <http://www.wikidata.org/entity/Q4167836> Wikimedia category
3506791 <http://www.wikidata.org/entity/Q16521> taxon
3292083 <http://www.wikidata.org/entity/Q523> star
2099663 <http://www.wikidata.org/entity/Q7318358> review article
2094945 <http://www.wikidata.org/entity/Q318> galaxy
1424106 <http://www.wikidata.org/entity/Q4167410> Wikimedia disambiguation page
1251868 <http://www.wikidata.org/entity/Q11173> chemical compound
1245714 <http://www.wikidata.org/entity/Q113145171> type of chemical entity
1215327 <http://www.wikidata.org/entity/Q7187> gene
 992852 <http://www.wikidata.org/entity/Q8054> protein
 797661 <http://www.wikidata.org/entity/Q11266439> Wikimedia template
 684774 <http://www.wikidata.org/entity/Q3305213> painting
 649585 <http://www.wikidata.org/entity/Q79007> street
 588561 <http://www.wikidata.org/entity/Q13100073> village
 578738 <http://www.wikidata.org/entity/Q13433827> encyclopedia article
 569277 <http://www.wikidata.org/entity/Q486972> human settlement
 555149 <http://www.wikidata.org/entity/Q101352> family name
 527496 <http://www.wikidata.org/entity/Q8502> mountain
 512972 <http://www.wikidata.org/entity/Q871232> editorial
 502740 <http://www.wikidata.org/entity/Q2668072> collection # TODO check
 419157 <http://www.wikidata.org/entity/Q4022> river
 391914 <http://www.wikidata.org/entity/Q30612> clinical trial
 353813 <http://www.wikidata.org/entity/Q1931185> astronomical radio source
 348470 <http://www.wikidata.org/entity/Q13406463> Wikimedia list article
 327763 <http://www.wikidata.org/entity/Q54050> hill
 292262 <http://www.wikidata.org/entity/Q1457376> eclipsing binary star
...


pv latest-all.nt.bz2.filtered | grep "<http://www\.wikidata\.org/prop/direct/P279>" | cut -d ' ' -f 3 | sort | uniq -c | sort -nr > subclass_stats
 945663 <http://www.wikidata.org/entity/Q20747295> protein-coding gene
 768954 <http://www.wikidata.org/entity/Q8054> protein
 453474 <http://www.wikidata.org/entity/Q7187> gene
  50018 <http://www.wikidata.org/entity/Q277338> pseudogene
  47847 <http://www.wikidata.org/entity/Q427087> non-coding RNA
  40201 <http://www.wikidata.org/entity/Q382617> mayor of a place in France
  40201 <http://www.wikidata.org/entity/Q15113603> municipal councillor
  14726 <http://www.wikidata.org/entity/Q11173> chemical compound
   8971 <http://www.wikidata.org/entity/Q22231119> CN - cycling race class defined by the International Cycling Union
   8883 <http://www.wikidata.org/entity/Q201448> transfer RNA
   8819 <http://www.wikidata.org/entity/Q64698614> pseudogenic transcript
   8024 <http://www.wikidata.org/entity/Q5663900> mayor of a place in Spain
   5245 <http://www.wikidata.org/entity/Q83307> minister
   4575 <http://www.wikidata.org/entity/Q11436> aircraft


Na samym końcu dodanie do kolekcji:
    '<http://www.wikidata.org/prop/direct/P1753>',  # list related to category
    '<http://www.wikidata.org/prop/direct/P1754>',  # category related to list
    '<http://www.wikidata.org/prop/direct/P18>',  # image
    '<http://www.wikidata.org/prop/direct/P948>',  # page banner
    '<http://schema.org/name>',
    '<http://www.w3.org/2000/01/rdf-schema#label>',
    '<http://schema.org/description>'


mapowanie tytuł -> id, id -> tytuł z redirectami

key -> value
https://github.com/Dobatymo/lmdb-python-dbm/tree/master

pv latest-all.nt.bz2.filtered | cut -d ' ' -f 1 | uniq > uniq_objects
wc -l uniq_objects
108801999
sort -u uniq_objects | wc -l
108801985

$ time sort uniq_objects | uniq -c | sort -nr | head -n 20
      2 <https://en.wikipedia.org/wiki/Template:Template_for_discussion/dated>
      2 <https://en.wikipedia.org/wiki/Radi%C4%8D>
      2 <https://en.wikipedia.org/wiki/Magic_systems_in_games>
      2 <https://en.wikipedia.org/wiki/Lynching_of_King_Johnson>
      2 <https://en.wikipedia.org/wiki/Kalmar_SS>
      2 <https://en.wikipedia.org/wiki/Hind_Institute_of_Medical_Sciences,_Barabanki>
      2 <https://en.wikipedia.org/wiki/Haven_Institute>
      2 <https://en.wikipedia.org/wiki/Good_Trouble>
      2 <https://en.wikipedia.org/wiki/Fauna_of_Bulgaria>
      2 <https://en.wikipedia.org/wiki/D._J._Taylor>
      2 <https://en.wikipedia.org/wiki/Darby_(Cambridgeshire_cricketer)>
      2 <https://en.wikipedia.org/wiki/Category:Pseudomallada>
      2 <https://en.wikipedia.org/wiki/Appenweier%E2%80%93Strasbourg_railway>
      2 <https://en.wikipedia.org/wiki/12th_Central_Committee_of_the_Communist_Party_of_Vietnam>
      1 <http://www.wikidata.org/entity/Q99999996>
      1 <http://www.wikidata.org/entity/Q99999994>
      1 <http://www.wikidata.org/entity/Q99999992>
      1 <http://www.wikidata.org/entity/Q99999990>
      1 <http://www.wikidata.org/entity/Q9999999>
      1 <http://www.wikidata.org/entity/Q99999989>

real    21m21,170s
user    35m25,420s
sys     0m42,147s
$ grep -E "^<https://en\.wikipedia\.org/wiki/Fauna_of_Bulgaria" latest-all.nt.bz2.filtered
<https://en.wikipedia.org/wiki/Fauna_of_Bulgaria> <http://schema.org/about> <http://www.wikidata.org/entity/Q59379839> .
<https://en.wikipedia.org/wiki/Fauna_of_Bulgaria> <http://schema.org/name> "Fauna of Bulgaria"@en .
<https://en.wikipedia.org/wiki/Fauna_of_Bulgaria> <http://schema.org/about> <http://www.wikidata.org/entity/Q117322559> .
<https://en.wikipedia.org/wiki/Fauna_of_Bulgaria> <http://schema.org/name> "Fauna of Bulgaria"@en .


shelve nie ogarnia tak dużego słownika
vedis OOM? - unmaintained
rocksdict - maintained
semidbm - index in memory, unmaintained
pysos - index in memory
berkeley?

rocksdict:
increase_parallelism


make filter
make dictrocks
make dictrocks_rev

make prepare_lists_and_categories
make download_members #run twice