## Airflow DAG для автоматизации загрузки GitHub Events

## Цель

Знакомство с Apache Airflow на практике: разработка DAG для автоматического запуска парсера GitHub API.

* Автоматический запуск скрипта каждые 5 минут
* Проверка, что данные действительно загрузились
* Логирование запусков в БД
* Ограничение по количеству запусков

## Структура проекта
airflow_dags/
├── dags/
│ ├── autoparse_github_events.py # DAG для автозапуска парсера
│ └── parser.py                  # Скрипт загрузки из GitHub API
├── logs/                        # Логи выполнения DAG-ов
├── data/
├── screenshots/
├── docker-compose.yml           # Конфигурация Airflow + PostgreSQL + pgAdmin
└── README.md

#### Технологии
* Apache Airflow (v2.9.3) — оркестрация
* PostgreSQL — метабаза Airflow и целевая БД
* pgAdmin — визуальное управление БД
* Docker & Docker Compose — контейнеризация

#### Концепции Airflow, применённые в проекте
|Концепция|Где используется|
|---------|----------------|
|DAG|	Определение workflow|
|PythonOperator|	Выполнение Python-функций|
|BashOperator|	Запуск внешнего скрипта|
|BranchPythonOperator|	Ветвление при достижении лимита|
|PostgresHook|	Подключение к БД|
|XCom|	Передача данных между тасками|
|Variables|	Хранение счётчика запусков|
|DummyOperator|	Точка объединения веток|

##№ Схема DAG
init_log_table → check_exec_limit
├── get_initial_count → run_parser → verify → log → end
└────────────────────────────────────────────→ end

#### Статусы тасков (последние 100 запусков)
![Статусы тасков](screenshots/dag_runs.png)

#### Таблица логов в pgAdmin
![Таблица логов](screenshots/dag_logs.png)

### Ключевые моменты из кода
```python
# Проверка лимита запусков с ветвлением
def check_exec_limit(**context):
    max_cnt = 10
    var_name = 'autoparse_github_runs'
    
    current_runs = int(Variable.get(var_name, default_var=0))
    
    if current_runs > max_cnt:
        return 'end' #останавливаем
      
    Variable.set(var_name, current_runs + 1)
    return 'get_initial_count' #продолжаем

# Проверка, что данные появились
def check_data_arrived(**context):
    hook = PostgresHook(postgres_conn_id='github_events_db')
    
    prev_cnt = context['ti'].xcom_pull(task_ids='get_initial_count')
    curr_cnt = hook.get_first(select_rowcount_sql)
    curr_cnt = curr_cnt[0]
    
    if prev_cnt == curr_cnt:
        raise Exception("Нет новых событий за последние 5 минут")
        
    print(f"Загружено новых событий: {curr_cnt - prev_cnt}")
```