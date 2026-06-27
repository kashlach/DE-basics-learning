'''
Тесты для loader
'''
from src.elt.loader import count_records, ensure_tables, get_engine, upsert_data


def clean_db():
    '''Очистка БД перед каждым тестом'''
    import src.elt.loader as loader
    loader._engine = None
    loader.DATABASE_URL = 'sqlite://'
    get_engine()
    ensure_tables()


def test_insert():
    '''Первая запись на дату'''
    clean_db()

    data = {'Valute': {'USD': {'Value': 89.5, 'Nominal': 1}}}
    result = upsert_data('27.06.2026', data)
    assert result == 'inserted'
    assert count_records() == 1

def test_update():
    '''Повторная запись на дату'''
    clean_db()

    data1 = {'Valute': {'USD': {'Value': 89.5, 'Nominal': 1}}}
    data2= {'Valute': {'USD': {'Value': 89.4, 'Nominal': 1}}}
    date = '27.06.2026'
    upsert_data(date, data1)
    result = upsert_data(date, data2)
    assert result == 'updated'
    assert count_records() == 1

def test_multiple_diff():
    '''Несколько записей за разные даты'''
    clean_db()

    data1 = {'Valute': {'USD': {'Value': 89.5, 'Nominal': 1}}}
    data2= {'Valute': {'USD': {'Value': 89.4, 'Nominal': 1}}}
    upsert_data('25.06.2026', data1)
    upsert_data('26.06.2026', data2)
    assert count_records() == 2

def test_multiple_same():
    '''Несколько записей за одну дату'''
    clean_db()

    data = {'Valute': {'USD': {'Value': 89.5, 'Nominal': 1}}}
    for _ in range(4):
        upsert_data('26.06.2026', data)

    assert count_records() == 1