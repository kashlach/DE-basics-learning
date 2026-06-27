'''
Получение курсов валют на дату от ЦБ РФ.
'''
import logging
import time

import requests

from src.elt.config import CBR_API_URL, MAX_RETRIES
from src.elt.xml_converter import xml_to_json

# логгер с именем, равным полному пути к текущему файлу, для понимания, из какого модуля сообщения
logger = logging.getLogger(__name__)

def build_url(target_date=None) -> str:
    if target_date is None:
        return CBR_API_URL

    date_str = target_date.strftime("%d/%m/%Y")
    return f"{CBR_API_URL}?date_req={date_str}"

def make_request(url) -> dict:
    logger.info(f"Запрос {url}")

    response = requests.get(url)

    if response.status_code == 404:
        logger.warning("Ошибка 404: нет данных")
        return None

    if response.status_code != 200:
        raise Exception(f"Ошибка: статус {response.status_code}")

    response.encoding = 'windows-1251'
    xml_string = response.text

    if not xml_string.strip().startswith('<?xml'):
        raise Exception("В ответе не XML")

    data = xml_to_json(xml_string)

    if "Valute" not in data:
        raise Exception("Ответ не содержит поле 'Valute'")

    return data

def make_retry(url) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            return make_request(url)
        except Exception:
            if attempt == MAX_RETRIES - 1:
               raise

            time.sleep(2 ** attempt)

def fetch_rates(target_date=None) -> dict:
    url = build_url(target_date)
    return make_retry(url)
