import os

ROW_LIMIT = 5000 # по-умолчанию 50000

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY")

db_uri = os.getenv("SQLALCHEMY_DATABASE_URI")
SQLALCHEMY_DATABASE_URI = db_uri # metadata БД

# включение поддержки шаблонизатора Jinja в SQL Lab и Explore
#FEATURE_FLAGS = {
#    "ENABLE_TEMPLATE_PROCESSING": True}

# кэширование данных в оперативной памяти процесса superset
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300  # время жизни кэша в секундах
}

# время ожидания ответа от веб-сервера
#SUPERSET_WEBSERVER_TIMEOUT = 60
 