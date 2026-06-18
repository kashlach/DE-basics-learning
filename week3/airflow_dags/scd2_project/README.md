## Реализация Slowly Changing Dimension (SCD) Type 2

Инкрементальная загрузка данных из источника (таблица Customers - источник бизнес-ключа customer_id и атрибутов: name, address, phone) в staging-слой и сохранение полной истории изменений атрибутов клиентов.


### Цель

Упрощенная реализация подхода к построению аналитического хранилища данных с сохранением полной истории изменений для практической отработки навыка оркестрации задач. 


**Что нового было освоено в рамках проекта**

* Работа с XCom для передачи данных между разными DAG'ами 
* Логирование через библиотеку `logging`
* DWH концепции (SCD Type 2, Staging-слой, Dimension-слой)
* ELT-подход


### Архитектура

Проект использует три основных слоя:

1. Источник (source) - имитация микросервиса с данными о клиентах, даг для генерации данных сгенерирован ИИ
2. Staging - промежуточный слой для хранения сырых данных
3. Dimension - слой с SCD Type 2 историей изменений


### Структура проекта

```text
airflow_dags/
├── dags/
│   ├── source_dag.py          # Генератор тестовых данных
│   ├── staging_dag.py         # Загрузка в staging-слой
│   └── scd2_dag.py            # SCD Type 2 обработка
├── logs/                      # Логи Airflow
├── data/                      # Данные (если нужны)
├── screenshots/               # Скриншоты для документации
├── docker-compose.yml         # Конфигурация Docker
└── README.md
```

---

##### Модель данных

Source (источник)
```sql
CREATE TABLE source.customers (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    address VARCHAR(255),
    phone VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE
);
```

Staging
```sql
CREATE TABLE scd2.customers_staging (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    address VARCHAR(255),
    phone VARCHAR(50),
    source_updated_at TIMESTAMP,
    _loaded_at_ TIMESTAMP DEFAULT NOW(),
    _ddl_code_ VARCHAR(1) DEFAULT 'I'  -- I=Insert, U=Update, D=Delete
);
```

Dimension (SCD2)
```sql
CREATE TABLE scd2.dim_customers (
    id INTEGER PRIMARY KEY DEFAULT nextval('scd2.surrogate_key'),
    customer_id INTEGER,
    name VARCHAR(100),
    address VARCHAR(255),
    phone VARCHAR(50),
    source_updated_at TIMESTAMP,
    created_timestamp TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);
```

Metadata (метаданные о загрузке)
```sql
CREATE TABLE scd2.metadata (
    table_name VARCHAR(100) PRIMARY KEY,
    last_loaded_at TIMESTAMP,
    max_source_ts TIMESTAMP  -- Watermark для инкрементальной загрузки
);
```


##### DAG'и проекта

---

1. *source_dag.py*  
Генерирует тестовые данные в таблице source.customers

###### Функциональность:

* Создание схемы и таблицы source.customers
* Генерация начальных 5 записей
* Имитация изменений каждые 3 минуты:
  * Добавление 0-3 новых клиентов
  * Обновление 0-2 существующих клиентов
  * Мягкое удаление 0-1 клиента
  * Возможность холостых запусков (15% вероятность)

![Граф source_dag](https://screenshots/source_dag_graph.png)

---

2. *staging_dag.py*
Выполняет инкрементальную загрузку данных из источника в staging-слой с использованием MERGE.

###### Функциональность:

* Создание схем и таблиц для staging
* Инкрементальная загрузка через MERGE
* Отслеживание изменений с помощью watermark
* Маркировка операций: I (insert), U (update), D (delete)
* Обновление метаданных о загрузке

![Граф staging_dag](https://screenshots/staging_dag_graph.png)

---

3. *scd2_dag.py*
Реализует SCD Type 2 логику: закрывает старые версии и создаёт новые при изменениях (через сочетание UPDATE и INSERT).

###### Функциональность:

* Создание dimension-таблицы с последовательностью для суррогатных ключей
* Закрытие старых версий (is_active = FALSE)
* Вставка новых версий (is_active = TRUE)
* Проверка целостности данных
* Отслеживание лага между staging и dim

![Граф scd2_dag](https://screenshots/scd2_dag_graph.png)