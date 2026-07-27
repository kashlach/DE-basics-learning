## GitHub Events ETL

Скрипт для получения[^1] 100 последних публичных событий из [GitHub Events API](https://api.github.com/events) (без авторизации), сохранения полученных данных в PostgreSQL - распарсенные поля в одну таблицу, сырой JSON в другую (JSONB), - и выполнения нескольких аналитических запросов.


### Цель

Знакомство со стеком: **API → Python → PostgreSQL → SQL-аналитика**. Задачей было только «потрогать» инструменты, а не построить осмысленный пайплайн. Все данные синтетические, а выборка нерепрезентативна — проект служит только для отработки технических навыков.


**Что было освоено в рамках проекта:**

* Запрос к **REST API** (библиотека `requests`, обработка JSON-ответа)
* Запуск **PostgreSQL** в **Docker** (контейнер из образа `postgres:15`, переменные окружения для пользователя/пароля/БД, прокидывание порта)
* Работа с БД из Python (**SQLAlchemy**, `to_sql()`, `text()`)
* Работа с `JSONB` в PostgreSQL (хранение и извлечение данных)
* Выполнение запросов через `docker exec` (запуск SQL-файлов и разовых команд в контейнере для отладки и выгрузки CSV)
* Осмысленное управление зависимостями ( `requirements.in с минимальными версиями → pip freeze > requirements.txt`)


### Структура проекта

```
github_events_etl/
├── requirements.in       # минимальные версии
├── requirements.txt      # зафиксированные версии
├── init.sql              # DDL (таблицы, индексы)
├── parser.py             # ETL-скрипт
├── sql/                  # отдельные селекты
│  ├── events_type.sql      # кол-во событий в разбивке по типам
│  ├── issues_action.sql    # action из payload из сырого json
│  └── payload_mdn_len.sql  # медианный размер секции payload в разбивке по типу события
├── reports/              # csv-отчеты как результат выполнения запросов из sql/ 
└── README.md
```


### Запуск

1. Запустить PostgreSQL:
```bash
docker run -d \
  --name pg-github-events \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=github_events_db \
  -p 5432:5432 \
  -v postgres_data:/var/lib/postgresql/data \
  postgres:15
```

```bash
docker cp init.sql pg-github-events:/init.sql
docker exec -it pg-github-events psql -U postgres -d github_events_db -f ./init.sql
```

2. Установить зависимости:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
3. Запустить ETL:

```bash
python parser.py
```
4. Выполнить SELECT'ы (опционально):

```bash
docker exec -i pg-github-events psql -U postgres -d github_events_db --csv < sql/top_repos.sql > reports/top_repos.csv
```
[^1]: Периодического сбора данных нет, это тема следующего проекта (Airflow).