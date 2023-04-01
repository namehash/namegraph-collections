


# List of Wikipedia articles

## 1. Download "List of" articles with Wikidata type

```
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT DISTINCT ?item ?type ?article (count(?articles) as ?count)
WHERE {
 ?item wdt:P360 ?type  . # is a list of
 ?article schema:about ?item .
 filter contains(str(?article),"en.wikipedia.org")
 ?articles schema:about ?item .
 filter contains(str(?articles),"wikipedia.org")
}
group by ?item ?type ?article
order by desc(?count)
```

110976 results in 30856 ms

`data/lists.json`:
```json
  {
    "item": "http://www.wikidata.org/entity/Q11750",
    "type": "http://www.wikidata.org/entity/Q3624078",
    "article": "https://en.wikipedia.org/wiki/List_of_sovereign_states",
    "count": "221"
  }
```

## 2. Download outgoing links to Wikipedia articles

`python scripts/download_category_members_and_links.py --mode list data/lists.json data/list_links.jsonl`


```json
{
  "item": "http://www.wikidata.org/entity/Q11750",
  "type": "http://www.wikidata.org/entity/Q3624078",
  "article": "https://en.wikipedia.org/wiki/List_of_sovereign_states",
  "count": "221",
  "members": [
    "List of sovereign states in the 1900s",
    "Mexico",
    "Tindouf",
    ...
  ]
}
```
# Wikipedia Categories

## 1. Download Categories with Wikipedia type

```
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT DISTINCT ?item ?type ?article 
WHERE {
 ?item wdt:P4224 ?type  . #category contains
 ?article schema:about ?item .
 filter contains(str(?article),"en.wikipedia.org")
}
```

462922 results in 59612 ms

`data/categories.json`:
```json
  {
    "item": "http://www.wikidata.org/entity/Q3919824",
    "type": "http://www.wikidata.org/entity/Q5",
    "article": "https://en.wikipedia.org/wiki/Category:Dutch_people"
  }
```

## 2. Download category members

`python scripts/download_category_members_and_links.py data/categories.json data/category_members.jsonl`

```json
{
  "item": "http://www.wikidata.org/entity/Q3919824",
  "type": "http://www.wikidata.org/entity/Q5",
  "article": "https://en.wikipedia.org/wiki/Category:Dutch_people",
  "members": [
    [
      1656551,
      "Dutch people"
    ]
  ]
}
```

# 3. For all articles (in lists and categories) get Wikidata ID

Parse `enwiki-latest-page_props.sql` and save `title`, `page_image_free`, `wikibase-shortdesc`, `wikibase_item`.

https://github.com/jcklie/wikimapper/tree/master
```
wikimapper download enwiki-latest --dir data
wikimapper create enwiki-latest --dumpdir data --target data/index_enwiki-latest.db
```

Note: Missing some data (thousands), e.g. Dhahbah, Avgustini - new pages

# 4. For each list/category filter members with list's/category's type
(>60s for 100 items)
```
SELECT DISTINCT ?item WHERE {
  ?item wdt:P31*/wdt:P279* wd:Q515
  FILTER (?item IN (wd:Q60,wd:Q61,wd:Q62,wd:5))
}
```
or (20s  for 100 items)
```
SELECT DISTINCT ?item WHERE {
  VALUES (?item) {(wd:Q60)(wd:Q61)(wd:Q62)(wd:Q5)}
  ?item wdt:P31*/wdt:P279* wd:Q515
}
```


Check types of all articles:
```
SELECT DISTINCT ?item ( GROUP_CONCAT ( DISTINCT ?instanceofs) AS ?instanceof ) WHERE {
  VALUES (?item) {(wd:Q6308514)(wd:Q1985398)(wd:Q7560003)(wd:Q43136178)(wd:Q5242252)(wd:Q42299795)(wd:Q28794165)(wd:Q43948389)(wd:Q4355517)(wd:Q6243936)(wd:Q26704319)(wd:Q55138785)(wd:Q106485454)(wd:Q6093728)(wd:Q104630569)(wd:Q27050778)(wd:Q15991719)(wd:Q4767735)(wd:Q102181978)(wd:Q22684197)(wd:Q7794710)(wd:Q59129868)(wd:Q16253640)(wd:Q86013286)(wd:Q22350829)(wd:Q16207381)(wd:Q65031039)(wd:Q43302483)(wd:Q256613)(wd:Q19667421)(wd:Q3369897)(wd:Q106588849)(wd:Q31189720)(wd:Q7349679)(wd:Q28454842)(wd:Q6409561)(wd:Q90054406)(wd:Q116275269)(wd:Q27922516)(wd:Q5494914)(wd:Q7343528)(wd:Q6780838)(wd:Q7387898)(wd:Q5213207)(wd:Q62026227)(wd:Q4023303)(wd:Q5651135)(wd:Q8054883)(wd:Q777563)(wd:Q16190847)(wd:Q7838441)(wd:Q48968302)(wd:Q5243780)(wd:Q4738114)(wd:Q6956807)(wd:Q99366200)(wd:Q1373096)(wd:Q16731667)(wd:Q4725023)(wd:Q7803067)(wd:Q17198148)(wd:Q4697197)(wd:Q4753855)(wd:Q94312885)(wd:Q29053555)(wd:Q73054564)(wd:Q88202895)(wd:Q6170905)(wd:Q19665684)(wd:Q66942486)(wd:Q267213)(wd:Q6308407)(wd:Q16732891)(wd:Q56651343)(wd:Q18350607)(wd:Q6137002)(wd:Q7292341)(wd:Q16209575)(wd:Q113861551)(wd:Q27452402)(wd:Q6213233)(wd:Q5044522)(wd:Q6380940)(wd:Q975203)(wd:Q460170)(wd:Q1297272)(wd:Q17775992)(wd:Q6859551)(wd:Q35780635)(wd:Q5571313)(wd:Q113371309)(wd:Q23770508)(wd:Q29359313)(wd:Q16929771)(wd:Q96741418)(wd:Q89357852)(wd:Q47502400)(wd:Q5638988)(wd:Q30069567)(wd:Q5046289)}
  ?item wdt:P31 ?instanceofs
  ?item wdt:wdt:P279 ?subclassofs
}
GROUP BY ?item
```

```
time python scripts/download_articles_types.py data/category_members.jsonl data/list_links.jsonl -o data/article_types.jsonl
time python scripts/download_articles_types.py data/category_members.jsonl data/list_links.jsonl -o data/article_types.jsonl -b 1000
```

`data/article_types.jsonl`:
```json
{
  "article": "Vigor Boucquet",
  "instanceof": [
    "Q5"
  ],
  "subclassof": []
}
```

Then validate types

```
python scripts/types_to_validate.py data/category_members.jsonl data/list_links.jsonl -a data/article_types.jsonl -o data/types_to_validate.json
```
```json
{
  "http://www.wikidata.org/entity/Q5": [
    "Q207293",
    "Q279283",
    "Q106377581",
    ...
  ]
}
```
```
Articles 10334978
Articles without instanceof and subclassof 2011952
Articles without instanceof 2181368
Articles without subclassof 9886940
```


### Validate types of articles as subclass of category/list type
```
python scripts/validate_types.py data/types_to_validate.json data/validated_types.jsonl
```
```json
{
  "type": "Q105416350", 
  "correct": [], 
  "incorrect": ["Q21484471", "Q3241972", "Q10617810"]
}
```

Filter articles 
```
python scripts/filter_articles.py data/category_members.jsonl data/article_types.jsonl data/validated_types.jsonl data/validated_category_members.jsonl
Members 21294548 valid, 7888585 invalid
python scripts/filter_articles.py data/list_links.jsonl data/article_types.jsonl data/validated_types.jsonl data/validated_list_links.jsonl
Members 7057739 valid, 25985431 invalid
```

# 5. Get page views of every list and category

Download from https://qrank.wmcloud.org/

```
python scripts/prepare_collections.py data/validated_list_links.jsonl qrank.csv data/list_links_collections.jsonl -n 110925
```
```
python scripts/prepare_collections.py data/validated_category_members.jsonl qrank.csv data/category_members_collections.jsonl -n 460127
```
```
time jq -s -c 'sort_by(.template.collection_rank)|reverse[]' data/list_links_collections.jsonl > data/list_links_collections_sorted.jsonl
time jq -s -c 'sort_by(.template.collection_rank)|reverse[]' data/category_members_collections.jsonl > data/category_members_collections_sorted.jsonl
```


# 6. How to get description and image for a category or list?

"The main article for this category is Apples." - TODO redirects?

https://en.wikipedia.org/wiki/Template:Cat_main

https://www.wikidata.org/wiki/Q11750 - page banner property


Properties:

## https://www.wikidata.org/wiki/Property:P1754 category related to list
list of songs composed by Franz Schubert (Q3154234) -> Category:Songs with music by Franz Schubert

SELECT DISTINCT ?item ?type
WHERE {
 ?item wdt:P1754 ?type  .
}
53532 results

## https://www.wikidata.org/wiki/Property:P18 image
SELECT DISTINCT ?item ?type ?image
WHERE {
?item wdt:P4224|wdt:P360 ?type .
?item wdt:P18 ?image
}
2024 results
SELECT DISTINCT ?item ?type ?image WHERE { ?item wdt:P18 ?image. }
many

## rdfs:label
list of songs composed by Franz Schubert

## schema:description

## https://www.wikidata.org/wiki/Property:P1753 list related to category (P1753)
SELECT DISTINCT ?item ?type
WHERE {
 ?item wdt:P1753 ?type  .
}
53681 results
SELECT DISTINCT ?item ?type
WHERE {
 ?item wdt:P1753 ?type  .
  ?type wdt:P1754 ?item .
}
53083 results


## Commons category (P373)
SELECT DISTINCT ?item ?type ?image
WHERE {
?item wdt:P4224|wdt:P360 ?type .
?item wdt:P373 ?image
}
232780 results

## For keywords: https://www.wikidata.org/wiki/Property:P971 category combines topics (P971)
SELECT DISTINCT ?item ?type ?image
WHERE {
?item wdt:P4224 ?type .
?item wdt:P971 ?image
}
929323 results

## page banner (P948)
SELECT DISTINCT ?item ?type ?image
WHERE {
?item wdt:P4224|wdt:P360 ?type .
?item wdt:P948 ?image
}
468 results
SELECT DISTINCT ?item ?type ?image WHERE { ?item wdt:P948 ?image. }
26793 results

## hashtag (P2572)
SELECT DISTINCT ?item ?type ?image
WHERE {
?item wdt:P4224|wdt:P360 ?type .
?item wdt:P2572 ?image
}
0 results
SELECT DISTINCT ?item ?type ?image
WHERE {
?item wdt:P2572 ?image
}
10385 results

## skos:altLabel


# Why 2 the same?
{"collection_name": "Highways in Poland", "collection_members": ["a4autostradapoland", "a8autostradapoland", "autostradaa2poland", "autostradaa1poland", "a1autostradapoland", "autostradaa4poland", "autostradaa6poland", "a18autostradapoland", "a6autostradapoland", "nationalroad18poland", "a2autostradapoland", "autostradaa18poland"], "collection_description": "", "collection_keywords": [], "collection_image": "", "metadata": {"collection_wikipedia_link": "https://en.wikipedia.org/wiki/Highways_in_Poland", "collection_wikidata_id": "Q926271", "collection_type_wikidata_id": "Q789026", "collection_articles": ["A4 autostrada (Poland)", "A8 autostrada (Poland)", "Autostrada A2 (Poland)", "Autostrada A1 (Poland)", "A1 autostrada (Poland)", "Autostrada A4 (Poland)", "Autostrada A6 (Poland)", "A18 autostrada (Poland)", "A6 autostrada (Poland)", "National road 18 (Poland)", "A2 autostrada (Poland)", "Autostrada A18 (Poland)"], "collection_rank": 625170}, "type": "template", "curated": false, "version": 0, "owner": "", "public": true, "category": "", "datetime": ""}
{"collection_name": "Highways in Poland", "collection_members": ["expressways61poland", "expressways12poland", "expressways51poland", "expressways2poland", "expressways74poland", "expressways10poland", "expressways1poland", "expressways79poland", "expressways19poland", "expressways22poland", "expressways11poland", "a3autostradapoland", "expressways14poland", "expressways17poland", "expressways86poland", "expressways5poland", "expressways7poland", "expressways16poland", "expressways8poland", "expressways69poland", "expressways3poland", "expressways6poland"], "collection_description": "", "collection_keywords": [], "collection_image": "", "metadata": {"collection_wikipedia_link": "https://en.wikipedia.org/wiki/Highways_in_Poland", "collection_wikidata_id": "Q926271", "collection_type_wikidata_id": "Q1127434", "collection_articles": ["Expressway S61 (Poland)", "Expressway S12 (Poland)", "Expressway S51 (Poland)", "Expressway S2 (Poland)", "Expressway S74 (Poland)", "Expressway S10 (Poland)", "Expressway S1 (Poland)", "Expressway S79 (Poland)", "Expressway S19 (Poland)", "Expressway S22 (Poland)", "Expressway S11 (Poland)", "A3 autostrada (Poland)", "Expressway S14 (Poland)", "Expressway S17 (Poland)", "Expressway S86 (Poland)", "Expressway S5 (Poland)", "Expressway S7 (Poland)", "Expressway S16 (Poland)", "Expressway S8 (Poland)", "Expressway S69 (Poland)", "Expressway S3 (Poland)", "Expressway S6 (Poland)"], "collection_rank": 625170}, "type": "template", "curated": false, "version": 0, "owner": "", "public": true, "category": "", "datetime": ""}