## Описание проекта

ETL-скрипт для анализа продаж из CSV-файлов. Объединяет данные о продажах с информацией о районах доставки, очищает данные и создаёт аналитические отчёты.


### Цель

Знакомство с основами ETL на синтетических данных.


**Что было освоено в рамках проекта:**

* ETL-пайплайн на Python
* Базовые операции pandas (`read_csv`, `groupby`, `merge`, `agg`)
* Создание виртуального окружения (`venv`) и файла `requirements.txt`
* Работа с Git и GitHub
* Markdown синтаксис для написания документации


### Структура проекта

```
pandas_simple_etl/
├── etl_script.py               # Основной ETL-скрипт
├── requirements.txt            # Зависимости Python
├── .gitignore
├── sales.csv                   # Исходные данные о продажах
├── districts.csv               # Данные о районе доставки заказа
├── data/                       # Обработанные данные
│ └── orders_and_districts.csv
├── reports/                    # Аналитические отчёты
│ ├── sales_by_category.csv
│ ├── sales_by_district.csv
│ └── top_products_by_district.csv
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