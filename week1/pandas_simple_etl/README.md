## Описание проекта

ETL-скрипт для анализа продаж из CSV-файлов. Объединяет данные о продажах с информацией о районах доставки, очищает данные и создаёт аналитические отчёты.

## Цель

Это учебный проект, в котором я знакомлюсь с основами ETL.

**Что я хотела освоить:**

* ETL-пайплайн
* Базовые операции pandas (read_csv, groupby, merge, agg)
* Виртуальное окружение и requirements.txt
* Git + GitHub
* Markdown синтаксис

## Структура проекта
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

## Запуск

##### Установка зависимостей
```bash
python -m venv venv
source venv/Scripts/activate  # Windows Git Bash
pip install -r requirements.txt
```
##### Выполнение
```bash
python etl_script.py
```