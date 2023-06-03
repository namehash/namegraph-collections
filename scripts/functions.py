import re
from urllib.parse import unquote

import myunicode
import regex
import requests
import rocksdict
import wikipediaapi
from SPARQLWrapper import SPARQLWrapper, JSON
from ens_normalize import DisallowedNameError, ens_cure
from rocksdict import AccessType
from unidecode import unidecode
from wikimapper import WikiMapper
from functools import wraps


def memoize_ram(original_function=None, path=None):
    if path is None:
        path = f'cache-{original_function.__name__}.rocks'

    try:
        cache = rocksdict.Rdict(path)
    except Exception as e:
        print(e)
        cache = rocksdict.Rdict(path, access_type=AccessType.read_only())

    cache2 = {}

    def _decorate(function):

        @wraps(function)
        def wrapper(*args, **kwargs):

            try:
                return cache2[args[0]]
            except KeyError:
                try:
                    result = cache[args[0]]
                    cache2[args[0]] = result
                    return result
                except KeyError:
                    result = function(*args, **kwargs)
                    cache2[args[0]] = result
                    cache[args[0]] = result
                    return result

        return wrapper

    if original_function:
        return _decorate(original_function)

    return _decorate


def memoize(original_function=None, path=None):
    if path is None:
        path = f'cache-{original_function.__name__}.rocks'

    cache = rocksdict.Rdict(path)

    def _decorate(function):

        @wraps(function)
        def wrapper(*args, **kwargs):
            try:
                return cache[args[0]]
            except KeyError:
                result = function(*args, **kwargs)
                cache[args[0]] = result
                return result

        return wrapper

    if original_function:
        return _decorate(original_function)

    return _decorate


class Member:
    def __init__(self, curated, tokenized):
        self.curated: str = curated
        self.tokenized = tokenized
        self.interesting_score: float | None = None
        self.rank: int | None = None
        self.status: str | None = None

    def json(self):
        return {
            'curated': self.curated,
            'tokenized': self.tokenized,
            'interesting_score': self.interesting_score,
            'rank': self.rank,
            'status': self.status,
        }

    @classmethod
    def from_dict(cls, member_data):
        member = cls(member_data['curated'], member_data['tokenized'])
        member.interesting_score = member_data['interesting_score']
        member.rank = member_data['rank']
        member.status = member_data['status']
        return member


class WikiAPI:
    def __init__(self):
        self.wiki = wikipediaapi.Wikipedia('en', extract_format=wikipediaapi.ExtractFormat.WIKI)
        self.sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        self.sparql.setMethod('POST')
        self.sparql.setReturnFormat(JSON)
        self.mapper: WikiMapper = None
        self.ens_cache = {}

    def init_wikimapper(self, wiki_mapper_path: str = 'data/index_enwiki-latest.db'):
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
    def extract_article_name(article: str) -> str:
        """
        Extracts article name from the link.
        :param article: link to the article
        :return: article name
        """
        if (not article.startswith('http://')) and (not article.startswith('https://')):
            return article

        m = re.match(r'https?://en\.wikipedia\.org/wiki/(.+)', article)
        return m.group(1)

    @staticmethod
    def extract_id(link: str) -> str:
        # assert link.startswith('http://www.wikidata.org/entity/Q')
        if link.startswith('http://www.wikidata.org/entity/Q'):
            return link[len('http://www.wikidata.org/entity/'):]
        return link

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
        name = WikiAPI.extract_article_name(collection_article)
        name = name.replace('_', ' ')
        name = unquote(name)
        name = regex.sub('^List of ', '', name)
        name = regex.sub('^Category:', '', name)
        name = name[0].upper() + name[1:]
        return name

    @staticmethod
    @memoize_ram
    def get_interesting_score(label):
        try:
            r = requests.post("https://ivc3ly7kt2yu3wekmsm5fqv4ku0hlstz.lambda-url.us-east-1.on.aws/inspector/",
                              json={'label': label, 'truncate_confusables': 0, 'truncate_graphemes': 0,
                                    'pos_lemma': False})
            rjson = r.json()
            interesting_score = rjson['interesting_score']
            tokenizations = rjson['tokenizations']
            try:
                best_tokenization = [token['token'] for token in tokenizations[0]['tokens']]
            except:
                best_tokenization = []
            return interesting_score, best_tokenization
        except:
            return None, []

    @staticmethod
    @memoize_ram
    def force_normalize(member):
        # member = member.replace('.', '')
        # TODO: remove () or use labels, e.g. Mary Poppins (film)
        # member = regex.sub(' *\(.*\)$', '', member)

        curated_token = ens_cure(member)
        curated_token2 = curated_token.replace('-', '')  # because other hyphens may be mapped
        curated_token2 = curated_token2.replace("'", '')

        # convert to ascii 

        if myunicode.script_of(curated_token2) in ('Common', 'Inherited', 'Latin'):
            curated_token3 = unidecode(curated_token2, errors='ignore')

            if curated_token3 != curated_token2:
                # print(curated_token2, curated_token3)
                curated_token2 = curated_token3

        if curated_token2 != curated_token:
            curated_token2 = ens_cure(curated_token2)

        return curated_token2
        # try:
        #     return self.ens_cache[member]
        # except KeyError:
        #     self.ens_cache[member] = ens_cure(member)
        #     return self.ens_cache[member]

    def curate_member(self, member) -> Member:
        member = unquote(member)
        member = member.replace('.', '')
        member = member.replace('-', '')
        member = member.replace("'", '')
        member = member.replace('"', '')
        member = regex.sub(' *\(.*\)$', '', member)
        try:
            curated = self.force_normalize(member)
            tokenized = []
            for token in member.split(' '):
                try:
                    curated_token = self.force_normalize(token)

                    tokenized.append(curated_token)
                except DisallowedNameError as e:
                    pass

            if len(curated) >= 3:
                return Member(curated, tokenized)
        except DisallowedNameError as e:
            print(member, e)
            return None

    def curate_members(self, members: list[str]) -> list[Member]:
        curated_members = []

        for member in members:
            member = self.curate_member(member)
            if member:
                curated_members.append(member)

        return curated_members

    def get_instances(self, wikidata_id: str, star: bool = False) -> list[dict]:
        """
        Returns instances of the type using Wikidata API. Filter out no entities (e.g. lexemes L...)
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

        res = []
        for item in results['results']['bindings']:
            try:
                res.append({
                    'id': WikiAPI.extract_id(item['instance']['value']),
                    'label': item['instanceLabel']['value'],
                })
            except AssertionError:
                continue
        return res
