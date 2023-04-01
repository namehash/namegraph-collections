import regex
import wikipediaapi
from SPARQLWrapper import SPARQLWrapper, JSON
from ens_normalize import ens_force_normalize, DisallowedLabelError
from wikimapper import WikiMapper


class WikiAPI:
    def __init__(self):
        self.wiki = wikipediaapi.Wikipedia('en', extract_format=wikipediaapi.ExtractFormat.WIKI)
        self.sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        self.sparql.setMethod('POST')
        self.sparql.setReturnFormat(JSON)
        self.mapper = None
        self.ens_cache = {}

    def init_wikimapper(self, wiki_mapper_path: str):
        self.mapper = WikiMapper(wiki_mapper_path)

    def category_members(self, category_name: str) -> list[str]:
        """
        Returns category members using Wikipedia API.
        :param category_name: with ot without "Category:" prefix
        :return: titles of Wikipedia articles in the category
        """
        members = set()

        if not category_name.startswith('Category:'):  # those are deleted categories anyway
            category_name = 'Category:' + category_name

        category = self.wiki.page(category_name, unquote=True)
        for member in self.wiki.categorymembers(category, cmnamespace=0).values():
            if member.ns == wikipediaapi.Namespace.MAIN:
                members.add(member.title)

        return list(members)

    def links(self, article_name: str) -> list[str]:
        """
        Returns links from the article using Wikipedia API.
        :param article_name: title of the article ("List of ...")
        :return: titles of Wikipedia articles linked from the article
        """
        links = set()

        article = self.wiki.page(article_name, unquote=True)
        for link in self.wiki.links(article, plnamespace=0).values():
            if link.ns == wikipediaapi.Namespace.MAIN:
                links.add(link.title)

        return list(links)

    def get_types_wikidata_ids(self, ids: list[str]) -> list[dict]:
        """
        Returns types of articles using Wikidata API.
        :param ids: list of Wikidata IDs
        :return: list of dicts with article Wikidata id, list of instance of types and list of subclass of types
        """
        members_str = ''.join([f'(wd:{id})' for id in ids])
        query = """SELECT DISTINCT ?item ( GROUP_CONCAT ( DISTINCT ?instanceofs) AS ?instanceof ) ( GROUP_CONCAT ( DISTINCT ?subclassofs) AS ?subclassof ) {{
      VALUES (?item) {{{members_str}}}
      OPTIONAL {{?item wdt:P31 ?instanceofs}} .
      OPTIONAL {{?item wdt:P279 ?subclassofs}}
    }} GROUP BY ?item""".format(members_str=members_str)

        self.sparql.setQuery(query)
        # self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        res = [{
            'article': self.extract_id(item['item']['value']),
            'instanceof': self._extract_ids(item['instanceof']['value'].split(' ')),
            'subclassof': self._extract_ids(item['subclassof']['value'].split(' '))
        } for item in results['results']['bindings']]

        return res

    @staticmethod
    def extract_id(link: str) -> str:
        return link.split('/')[-1]  # TODO fix?

    @staticmethod
    def _extract_ids(links: list[str]) -> list[str]:
        return [WikiAPI.extract_id(link) for link in links if link]

    def get_types(self, articles: list[str]) -> list[dict]:
        """
        Returns types of articles using Wikidata API.
        :param articles: list of article names
        :return: list of dicts with article name, list of instance of types and list of subclass of types
        """
        # convert article names to wikidata id
        members_ids = {}
        for member in articles:
            wikidata_id = self.mapper.title_to_id(member.replace(' ', '_'))
            if wikidata_id is not None:
                members_ids[wikidata_id] = member

        article_types = self.get_types_wikidata_ids(members_ids.keys())

        # convert article names to wikidata ids and save also articles without wikidata id
        validated_articles = set()
        for type in article_types:
            type['article'] = members_ids[type['article']]
            validated_articles.add(type['article'])

        for article in set(articles) - validated_articles:
            article_types.append({
                'article': article,
                'instanceof': [],
                'subclassof': []
            })

        return article_types

    def validate_types(self, type_id: str, article_types_ids: list[str], forward: bool = False) -> tuple[
        list[str], list[str]]:
        """
        Validates types of articles using Wikidata API.
        :param type_id: Wikidata ID of the type
        :param article_types_ids: list of Wikidata IDs of types of articles
        :param forward: 
        :return: list of correct and incorrect article types
        """
        correct = []
        incorrect = []

        if forward:
            func = lambda id: self._validate_wikidata_ids(id, article_types_ids)
        else:
            func = self._get_subclasses

        correct_ids = set([id_link.split('/')[-1] for id_link in func(type_id)])
        # print('correct_ids', correct_ids)

        # print(members_ids)
        for id in article_types_ids:
            if id in correct_ids:
                correct.append(id)
            else:
                incorrect.append(id)

        # print('correct', correct)
        # print('incorrect', incorrect)
        # print()

        return correct, incorrect

    def _get_subclasses(self, type_id: str) -> list[str]:
        """
        Returns subclasses of the type using Wikidata API.
        :param type_id: Wikidata ID of the type
        :return: list of Wikidata IDs of subclasses
        """
        query = """SELECT DISTINCT ?item WHERE {{
          ?item wdt:P279* wd:{type_id}
        }}""".format(type_id=type_id)

        self.sparql.setQuery(query)
        # self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        res = [item['item']['value'] for item in results['results']['bindings']]
        return res

    def _validate_wikidata_ids(self, type_id: str, ids: list[str]):
        """
        Returns article type ids which are subclasses of the type using Wikidata API.
        :param type_id: Wikidata ID of the type
        :param ids: list of Wikidata IDs of types of articles
        :return: list of Wikidata IDs of article types which are subclasses of the type
        """
        members_str = ''.join([f'(wd:{id})' for id in ids])
        query = """SELECT DISTINCT ?item WHERE {{
      VALUES (?item) {{{members_str}}}
      ?item wdt:P279* wd:{type_id}.
      hint:Prior hint:gearing "forward".
    }}""".format(type_id=type_id, members_str=members_str)

        self.sparql.setQuery(query)
        # self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        # print(query)

        res = [item['item']['value'] for item in results['results']['bindings']]
        return res

    @staticmethod
    def curate_name(collection_article: str):
        name = WikiAPI.extract_id(collection_article)
        name = name.replace('_', ' ')
        name = regex.sub('^List of ', '', name)
        name = regex.sub('^Category:', '', name)
        name = name[0].upper() + name[1:]
        return name

    def force_normalize(self, member):
        # member = member.replace('.', '')
        # TODO: remove () or use labels, e.g. Mary Poppins (film)
        # member = regex.sub(' *\(.*\)$', '', member)

        try:
            return self.ens_cache[member]
        except KeyError:
            self.ens_cache[member] = ens_force_normalize(member)
            return self.ens_cache[member]

    def curate_members(self, members: list[str]) -> list[str, list[str]]:
        curated_members = []

        for member in members:
            member = member.replace('.', '')
            member = regex.sub(' *\(.*\)$', '', member)
            try:
                curated = self.force_normalize(member)
                tokenized = []
                for token in member.split(' '):
                    try:
                        curated_token = self.force_normalize(token)
                        tokenized.append(curated_token)
                    except DisallowedLabelError as e:
                        pass

                if len(curated) >= 3:
                    curated_members.append((curated, tokenized))
            except DisallowedLabelError as e:
                print(member, e)

        return curated_members

    def get_instances(self, wikidata_id: str, star: bool = False) -> list[dict]:
        """
        Returns instances of the type using Wikidata API.
        :param wikidata_id: Wikidata ID of the type
        :param star: if True, returns also instances of subclasses
        :return: list of instances with their Wikidata IDs and labels
        """
        if star:
            property = 'wdt:P31/wdt:P279*'
        else:
            property = 'wdt:P31'
        query = f"""SELECT ?instance ?instanceLabel WHERE {{
      ?instance {property} wd:{wikidata_id}. 
      SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "en" .
       }}
    }}"""

        self.sparql.setQuery(query)

        results = self.sparql.query().convert()

        res = [{
            'id': WikiAPI.extract_id(item['instance']['value']),
            'label': WikiAPI.extract_id(item['instanceLabel']['value']),
        } for item in results['results']['bindings']]

        return res
