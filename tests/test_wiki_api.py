from scripts.functions import WikiAPI
import pytest


def test_category_members():
    """Test category members."""
    wiki = WikiAPI()
    members = wiki.category_members('Category:Nobel_Peace_Prize_laureates')
    assert 'Adolfo Pérez Esquivel' in members
    assert "Paul Henri Balluet d'Estournelles de Constant" in members
    assert 'Betty Williams (peace activist)' in members

    members = wiki.category_members('Category:Children%27s_writers')
    assert 'Pipaluk Freuchen' in members

    members = wiki.category_members("Category:Children's writers")
    assert 'Pipaluk Freuchen' in members

    members = wiki.category_members("Children's writers")
    assert 'Pipaluk Freuchen' in members


# TODO test redirects
def test_links():
    """Test links."""
    wiki = WikiAPI()
    links = wiki.links('List_of_sovereign_states')
    assert 'Sri Lanka' in links

    links = wiki.links('List of sovereign states')
    assert 'Sri Lanka' in links


def test_get_types_wikidata_ids():
    """Test get types wikidata ids."""
    wiki = WikiAPI()
    ids = ['Q5', 'Q123']
    types = wiki.get_types_wikidata_ids(ids)
    assert types == [
        {'article': 'Q5', 'instanceof': ['Q55983715'], 'subclassof': ['Q154954', 'Q164509', 'Q215627', 'Q110551885']},
        {'article': 'Q123', 'instanceof': ['Q47018901'], 'subclassof': ['Q18602249']}]


def test_extract_id():
    """Test extract id."""
    assert WikiAPI.extract_id('http://www.wikidata.org/entity/Q5') == 'Q5'
    assert WikiAPI.extract_id('http://www.wikidata.org/entity/Q123') == 'Q123'


def test_extract_ids():
    """Test extract ids."""
    assert WikiAPI._extract_ids(['http://www.wikidata.org/entity/Q5', 'http://www.wikidata.org/entity/Q123']) == ['Q5',
                                                                                                                  'Q123']


def test_extract_article_name():
    """Test extract article name."""
    assert WikiAPI.extract_article_name(
        'https://en.wikipedia.org/wiki/Adolfo_P%C3%A9rez_Esquivel') == 'Adolfo_P%C3%A9rez_Esquivel'
    assert WikiAPI.extract_article_name(
        'http://en.wikipedia.org/wiki/Adolfo_Pérez_Esquivel') == 'Adolfo_Pérez_Esquivel'


def test_get_types():
    """Test get types."""
    wiki = WikiAPI()
    wiki.init_wikimapper()
    articles = ['Apple', 'Betty Williams (peace activist)']
    types = wiki.get_types(articles)
    assert types == [{'article': 'Betty Williams (peace activist)', 'instanceof': ['Q5'], 'subclassof': []},
                     {'article': 'Apple', 'instanceof': [], 'subclassof': ['Q3314483']}]


def test_get_types_encoded():
    """Test get types."""
    wiki = WikiAPI()
    wiki.init_wikimapper()
    types = wiki.get_types(['Le_Monde%27s_100_Books_of_the_Century'])
    assert types == [
        {'article': "Le_Monde%27s_100_Books_of_the_Century", 'instanceof': ['Q1046088', 'Q13406463', 'Q28050019'],
         'subclassof': []}]


def test_get_types_without_spaces():
    """Test get types."""
    wiki = WikiAPI()
    wiki.init_wikimapper()
    types = wiki.get_types(['Todd_Donoho'])
    assert types == [{'article': 'Todd_Donoho', 'instanceof': ['Q5'], 'subclassof': []}]


def test_get_types_not_existing():
    """Test get types."""
    wiki = WikiAPI()
    wiki.init_wikimapper()
    types = wiki.get_types(['oruyhgbsodgby084bg0pws8e7gosdifbg'])
    assert types == [{'article': 'oruyhgbsodgby084bg0pws8e7gosdifbg', 'instanceof': [], 'subclassof': []}]


def test__get_subclasses():
    """Test get subclasses."""
    wiki = WikiAPI()
    subclasses = wiki._get_subclasses('Q103820137')
    assert subclasses == ['http://www.wikidata.org/entity/Q2901352',
                          'http://www.wikidata.org/entity/Q5093326',
                          'http://www.wikidata.org/entity/Q103820137', ]
    # TODO change to IDs only


def test__validate_wikidata_ids():
    """Test validate wikidata ids."""
    wiki = WikiAPI()
    ids = ['Q2901352', 'Q5093326', 'Q103820137', 'Q43229']
    assert wiki._validate_wikidata_ids('Q103820137', ids) == ['http://www.wikidata.org/entity/Q2901352',
                                                              'http://www.wikidata.org/entity/Q5093326',
                                                              'http://www.wikidata.org/entity/Q103820137', ]
    # TODO change to IDs only


def test_validate_types():
    """Test validate types."""
    wiki = WikiAPI()
    ids = ['Q2901352', 'Q5093326', 'Q103820137', 'Q43229']
    assert wiki.validate_types('Q103820137', ids, forward=False) == (['Q2901352', 'Q5093326', 'Q103820137'], ['Q43229'])


def test_validate_types_forward():
    """Test validate types."""
    wiki = WikiAPI()
    ids = ['Q2901352', 'Q5093326', 'Q103820137', 'Q43229']
    assert wiki.validate_types('Q103820137', ids, forward=True) == (['Q2901352', 'Q5093326', 'Q103820137'], ['Q43229'])


def test_curate_name():
    """Test curate name."""
    wiki = WikiAPI()
    assert wiki.curate_name('Adolfo_Pérez_Esquivel') == 'Adolfo Pérez Esquivel'
    assert wiki.curate_name('Category:Nobel_Peace_Prize_laureates') == 'Nobel Peace Prize laureates'
    assert wiki.curate_name('List_of_sovereign_states') == 'Sovereign states'


def test_curate_members():
    """Test curate members."""
    wiki = WikiAPI()
    members = wiki.curate_members(['Adolfo_Pérez_Esquivel', 'Betty Williams (peace activist)', 'ιοσρβυνγ'])
    assert members == [('adolfopérezesquivel', ['adolfopérezesquivel']),
                       ('bettywilliams', ['betty', 'williams'])]


def test_get_instances_no_labels():
    """Test get instances."""
    wiki = WikiAPI()
    instances = wiki.get_instances('Q5269211')
    print(instances)
    assert instances == [{'id': 'Q12991635', 'label': 'Q12991635'}, {'id': 'Q12991638', 'label': 'Q12991638'},
                         {'id': 'Q56071535', 'label': 'Q56071535'}, {'id': 'Q56071536', 'label': 'Q56071536'}]


def test_get_instances():
    """Test get instances."""
    wiki = WikiAPI()
    instances = wiki.get_instances('Q866')
    print(instances)
    assert instances == [{'id': 'Q133363', 'label': 'Satanism'}, {'id': 'Q4661404', 'label': 'Aalon'},
                         {'id': 'Q55605876', 'label': 'Brat'}]


def test_get_instances_all2():
    """Test get instances."""
    wiki = WikiAPI()
    instances = wiki.get_instances('Q733553', star=True)  # TODO what to do with lexemes?
    print(instances)
    assert instances == [{'id': 'Q133363', 'label': 'Satanism'}, {'id': 'Q4661404', 'label': 'Aalon'},
                         {'id': 'Q55605876', 'label': 'Brat'}]


def test_get_instances_all():
    """Test get instances."""
    wiki = WikiAPI()
    instances = wiki.get_instances('Q102345381', star=False)
    assert {'id': 'Q111792308', 'label': 'Life at Google'} in instances
    assert {'id': 'Q109527889', 'label': 'BotezLive'} not in instances

    instances2 = wiki.get_instances('Q102345381', star=True)
    assert len(instances2) >= len(instances)
    assert {'id': 'Q111792308', 'label': 'Life at Google'} in instances2
    assert {'id': 'Q109527889', 'label': 'BotezLive'} in instances2
