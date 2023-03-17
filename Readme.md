


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
```
  {
    "item": "http://www.wikidata.org/entity/Q11750",
    "type": "http://www.wikidata.org/entity/Q3624078",
    "article": "https://en.wikipedia.org/wiki/List_of_sovereign_states",
    "count": "221"
  }
```

## 2. Download outgoing links to Wikipedia articles

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
```
  {
    "item": "http://www.wikidata.org/entity/Q3919824",
    "type": "http://www.wikidata.org/entity/Q5",
    "article": "https://en.wikipedia.org/wiki/Category:Dutch_people"
  }
```

## 2. Download category members

# 3. For all articles (in lists and categories) get Wikidata ID

# 4. For each list/category filter out members with list's/category's type
