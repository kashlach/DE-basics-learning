import os

# значение = os.getenv('ИМЯ_ПЕРЕМЕННОЙ', 'ЗначениеПоУмолчанию')
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///dev_data.db")


# Официальный XML API ЦБ РФ
CBR_API_URL = "http://www.cbr.ru/scripts/XML_daily.asp"

# К-во ретраев запросов
MAX_RETRIES = 3
