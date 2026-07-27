## Описание проекта

ETL-скрипт для анализа продаж из CSV-файлов с данными о продажах и информацией о районах доставки заказов:
- загрузка данных из файлов
- обеспечение качества данных: дедубликация, валидация суммы заказа (>= 0), форматирование строковых данных
- объединение подготовленных датасетов
- формирование csv с аналитическими отчетами

### Цель

Знакомство с основами ETL, работа с pandas.

**Что было освоено в рамках проекта:**

* ETL-пайплайн на Python
* Базовые операции pandas (`read_csv`, `groupby`, `merge`, `agg`)
* Создание виртуального окружения (`venv`) и файла `requirements.txt`
* Работа с Git и GitHub
* Markdown синтаксис для написания документации

### Структура проекта

```
pandas_simple_etl/
├── etl_script.py               # ETL-скрипт
├── requirements.txt            # Зависимости Python
├── sales.csv                   # Исходные данные о продажах
├── districts.csv               # Данные о районе доставки заказа
├── data/                       # Обработанные данные
│  └── orders_and_districts.csv
├── reports/                    # Аналитические отчеты
│  ├── sales_by_category.csv
│  ├── sales_by_district.csv
│  └── top_products_by_district.csv
└── README.md
```

### Запуск

1. Установить зависимости:
```bash
python -m venv venv
source venv/Scripts/activate  # Windows Git Bash
pip install -r requirements.txt
```

2. Выполнить ETL-скрипт
```bash
python etl_script.py
```