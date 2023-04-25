import rocksdict
from pytest import mark
from rocksdict import AccessType
from wikimapper import WikiMapper


def test_q5_label():
    db5 = rocksdict.Rdict('data/db5.rocks', access_type=AccessType.read_only())
    assert db5['Q5']['label'] == 'human'


# https://en.wikipedia.org/w/index.php?title=List_of_Dune_planets&redirect=no ma więcej planet, np. Caladan
@mark.xfail
def test():
    mapper = WikiMapper('data/index_enwiki-latest.db')
    wikidata_id = mapper.title_to_id('Caladan')
    print(wikidata_id)
    db1 = rocksdict.Rdict('data/db1.rocks', access_type=AccessType.read_only())
    wikidata_id2 = db1['List_of_Dune_planets']['about']
    print(wikidata_id2)
    assert wikidata_id == 'Q2445640'


def test_quoting_mapper():
    mapper = WikiMapper('data/index_enwiki-latest.db')
    assert mapper.title_to_id('Adolfo_Pérez_Esquivel') == 'Q206505'
    assert mapper.title_to_id('Adolfo_P%C3%A9rez_Esquivel') is None
    assert mapper.title_to_id('Adolfo Pérez Esquivel') is None
    for title in ['Category:Children%27s_writers', "Category:Children's writers", "Category:Children's_writers",
                  "Children's writers",
                  'Adolfo_P%C3%A9rez_Esquivel', 'Adolfo_Pérez_Esquivel', 'Adolfo Pérez Esquivel', 'Category:Military']:
        wikidata_id = mapper.title_to_id(title)
        print(title, wikidata_id)

    # TODO add categories to wikimapper


def test_quoting_db1():
    db1 = rocksdict.Rdict('data/db1.rocks', access_type=AccessType.read_only())

    assert db1['Category:Children%27s_writers']['about'] == 'Q788499'
    assert "Category:Children's writers" not in db1['Category:Children%27s_writers']
    assert "Children's writers" not in db1['Category:Children%27s_writers']
