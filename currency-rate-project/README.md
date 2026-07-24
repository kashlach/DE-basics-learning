## ELT-пайплайн для курсов валют ЦБ РФ

Сбор, трансформация и визуализация данных о курсах валют ЦБ РФ. Проект автоматизирует процесс получения данных через API, загрузку сырых данных в базу, трансформацию через dbt и визуализацию в Grafana, а также формирование отчета excel и алерта c отправкой по email.

### Цель

Собрать в одном проекте инструменты из предыдущих + добавить новые (`uv`, `dbt`, `grafana`). Посмотреть на их работу в связке друг с другом.

**Что нового было освоено в рамках проекта**

* **uv** как замена pip и venv
* **ruff** для форматирования python кода
* **pytest** для тестирования python кода
* **Dockerfile** для создания образа Airflow с dbt
* **dbt** для трансформации данных в хранилище
* **grafana** для визуализации полученных данных

### Как это работает (общая схема)

```text
API ЦБ РФ
↓
Airflow DAG
↓
PostgreSQL raw слой (тут сырой ответ в JSONB)
↓
dbt: staging → intermediate → marts
↓
дашборды в графане / отчет и алерт на почту
```

**Стек:** python 3.11+ (uv, ruff), airflow, dbt, postgres (jsonb), sqlite (для разработки без docker), grafana

### Структура проекта

```text
currency-rate-project/
├── airflow/
│  ├── dags/
│  │  └── currency_pipeline.py  # dag
│  └── reports/                 # еженедельные excel отчеты
│     └── svod_auto_report.xlsx # PQ-шаблон для сборки сводного отчета
├── currency_dbt/               # dbt проект
│  ├── dbt_project.yml
│  ├── packages.yml             # dbt-utils макросы для тестирования данных
│  ├── profiles.yml
│  └── models/
│     ├── staging/
│     ├── intermediate/
│     └── marts/ 
├── grafana/provisioning/
│  ├── alerting/
│  │  ├── contact_points.yaml
│  │  ├── policies.yaml
│  │  └── alert_rules.yaml
│  ├── dashboards/
│  │  ├── dashboards.yml
│  │  └── json/                 # json дашбордов, автоматически импортируются в графану
│  │     ├── business/          # бизнес-дашборды
│  │     │  ├── currency_dash.json
│  │     │  ├── usd_dash.json
│  │     │  └── thb_dash.json
│  │     └── tech/              # технические дашборды
│  │        └── etl_monitoring.json
│  └── datasources/
│     └── postgres.yaml
├── src/elt/
│  ├── api_client.py            # запросы курсов с ретраями
│  ├── xml_converter.py         # xml в json
│  ├── loader.py                # upsert в БД
│  ├── config.py                # константы
│  └── run.py                   # ручной запуск для отладки
├── tests/
│  ├── test_api_client.py
│  └── test_loader.py
├── sql/
│  ├── init_airflow_db.sql
│  ├── init_airflow_reader.sql
│  └── init_grafana_user.sql
├── screenshots/
├── Dockerfile                 # образ airflow:2.9.3 + dbt-postgres
├── docker-compose.yml         # postgres, airflow, pgadmin, grafana
├── pyproject.toml             # зависимости (uv)
├── .env.example
└── README.md
```
##### Схема DAG и статусы тасков
![Схема DAG](screenshots/currency_pipeline_dag.png)

##### Граф зависимостей dbt

![Граф dbt](screenshots/dbt_graph.png)

##### Документация dbt (staging-слой)

![staging-слой dbt](screenshots/dbt_stg_description.png)

---
### Запуск:

```bash
# 1. Клонируем
git clone https://github.com/ваш-логин/currency-rate-project.git
cd currency-rate-project

# 2. Настраиваем .env (нужно отредактировать)
cp .env.example .env

# 3. Устанавливаем зависимости
uv sync

# 4. Собираем образ airflow с dbt (перед первым запуском)
docker build -t my-airflow-with-dbt .

# 5. Запускаем контейнеры
docker-compose up -d
```
После запуска:

| Сервис	| URL	| Логин / Пароль |
|-|-|-|
|Airflow	|http://localhost:8080	|admin / admin|
|Grafana	|http://localhost:3000	|admin / admin|
|pgAdmin	|http://localhost:5050	|admin@example.com / admin|

В Airflow включаем DAG currency_pipeline. Он настроен на ежедневное выполнение в 10:00 UTC.

---
#####  Отчетность

1. Еженедельный excel отчет
- Путь: `./airflow/reports/`
- Имя файла: `weekly_report_YYYY-MM-DD[_manual].xlsx`
- Режимы запуска:
  - *Автоматический* (по расписанию): формируется по понедельникам, данные за прошедшую неделю пн-вс, отправляется на почту (если настроен SMTP в airflow)
  - *Ручной*: формируется в любой день недели, в имя файла добавляется суффикс `_manual`, не отправляется на почту

2. Сводный отчет (через Power Query)
Содержит таблицу со списком валют и средним значением курса в разрезе номеров недель.

- Путь: `./airflow/reports/`
- Имя файла: `svod_auto_report.xlsx`
- Источник данных: автоматически сформированные еженедельные отчеты

3. Алерт по USD
- Проверяет изменение >2% за сутки
- Пишет в лог и отправляет уведомление на почту (если настроен SMTP в airflow). Уведомление содержит:
  - Заголовок с направлением изменения (повышение или снижение)
  - Предыдущее и текущее значение курса
  - Изменение в процентах


###### Настройка SMTP через Connection:

Conn Id: smtp_default

Conn Type: smtp

Host: smtp.yandex.ru (или другой)

Port: 465

Login: адрес почты (например your_login@yandex.ru)

Password: пароль приложения (не от почты)

Disable TLS: проставить галочку

---
#####  Мониторинг состояния ETL

Для контроля состояния пайплайна добавлен **дашборд ETL Healthcheck**. Источник данных: метабаза Airflow

![ETL Healthcheck dashboard](screenshots/etl_healthcheck_dash.png)

Также настроен **алерт**, который срабатывает, если последний успешный запуск дага `currency_pipeline` был более 24 часов назад, с уведомлением на email (замените адрес почты recipient_login@example.com в `grafana/provisioning/alerting/contact_points.yaml` на нужный).

---
#####  Тесты

test_api_client.py - проверка URL и формата даты

test_loader.py - проверка upsert-логики

```bash
uv run pytest tests/ -v
```

---
###  Что можно улучшить

* инкрементальные модели dbt
  сейчас тип материализации table, т.к. данных мало (~20 тыс строк в год), при росте может иметь смысл перевести на incremental чтобы не пересобирать таблицы каждый день
* ~~добавить дашборд в графане для мониторинга elt на основе данных метабазы эйрфлоу~~
* добавить makefile
* добавить больше тестов