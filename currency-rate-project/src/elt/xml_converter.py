"""
Конвертация XML-ответа ЦБ РФ в JSON.
"""
import logging
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


def xml_to_json(xml_string):
    """
    Преобразует XML от ЦБ в JSON.

    Вход:
    <?xml version="1.0" encoding="windows-1251"?>
    <ValCurs Date="15.01.2024" name="Foreign Currency Market">
        <Valute ID="R01235">
            <CharCode>USD</CharCode>
            <Nominal>1</Nominal>
            <Name>Доллар США</Name>
            <Value>89,50</Value>
        </Valute>
    </ValCurs>

    Выход:
    {
        "Date": "2024-01-15T00:00:00+03:00",
        "Valute": {
            "USD": {"CharCode": "USD", "Value": 89.50, "Nominal": 1, "Name": "Доллар США"}
        }
    }
    """
    root = ET.fromstring(xml_string)

    # Дата из атрибута: "15.01.2024" -> "2024-01-15"
    raw_date = root.attrib.get("Date", "")
    parts = raw_date.split(".")
    if len(parts) == 3:
        formatted_date = f"{parts[2]}-{parts[1]}-{parts[0]}T00:00:00+03:00"
    else:
        formatted_date = raw_date

    # Собираем валюты
    valute = {}
    for elem in root.findall("Valute"):
        char_code = elem.find("CharCode").text

        # Запятая -> точка: "89,50" -> 89.50
        value_str = elem.find("Value").text
        value = float(value_str.replace(",", "."))

        nominal = int(elem.find("Nominal").text)
        name = elem.find("Name").text

        valute[char_code] = {
            "CharCode": char_code,
            "Value": value,
            "Nominal": nominal,
            "Name": name,
        }

    logger.info(f"Конвертировано валют: {len(valute)}")

    return {
        "Date": formatted_date,
        "Valute": valute,
    }
