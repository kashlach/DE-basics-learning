'''
Тесты для api_client.
'''
from datetime import date

from src.elt.api_client import build_url


def test_url_without_date():
    '''проверяем, что функция build_url возвращает то, что в конфиге'''
    url = build_url(None)
    assert url == "http://www.cbr.ru/scripts/XML_daily.asp"

def test_url_with_date():
    '''с датой проверяем, что добавился параметр'''
    test_date = date(2026, 6, 24)
    url = build_url(test_date)
    assert "?date_req=24/06/2026" in url

def test_url_end_of_year():
    '''проверяем, что дата подставляется в нужном формате'''
    test_date = date(2025, 12, 31)
    url = build_url(test_date)
    assert url.endswith("31/12/2025")

