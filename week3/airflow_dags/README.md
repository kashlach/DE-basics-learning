## Airflow DAG для автоматизации загрузки GitHub Events

Периодический сбор данных по [GitHub Events API](https://api.github.com/events) с помощью скрипта из предыдущего проекта. DAG автоматически запускает парсер каждые 5 минут и логирует запуски в БД.


### Цель

Знакомство с Apache Airflow для оркестрации задач на практике.

**Что нового было освоено в рамках проекта**

* Контейнеризация с помощью **Docker Compose**
* Взаимодействие с PostgreSQL через `PostgresHook`
* Разработка DAG в Airflow
* Использование `PythonOperator`, `BashOperator` и `BranchPythonOperator`
* Обмен данными между задачами через `XCom`
* Использование `Variables` (счётчик запусков)
* Мониторинг выполнения тасков через Web UI Airflow
* Работа с pgAdmin

### Структура проекта

```text
airflow_dags/
├── dags/
│ ├── autoparse_github_events.py # DAG для автозапуска парсера
│ └── parser.py                  # Скрипт загрузки из GitHub API
├── logs/                        # Логи выполнения DAG
├── screenshots/                 
├── docker-compose.yml           # Airflow + PostgreSQL + pgAdmin
└── README.md
```

---

##### Схема DAG

```text
init_log_table → check_exec_limit
├── get_initial_count → run_parser → verify → log → end
└────────────────────────────────────────────→ end
```

##### Статусы тасков (последние 100 запусков)

![Статусы тасков](screenshots/dag_runs.png)


##### Таблица логов в pgAdmin

![Таблица логов](screenshots/dag_logs.png)


##### Ключевые моменты из кода

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

# Проверка, что данные были загружены
def check_data_arrived(**context):
    hook = PostgresHook(postgres_conn_id='github_events_db')
    
    prev_cnt = context['ti'].xcom_pull(task_ids='get_initial_count')
    curr_cnt = hook.get_first(select_rowcount_sql)
    curr_cnt = curr_cnt[0]
    
    if prev_cnt == curr_cnt:
        raise Exception("Нет новых событий за последние 5 минут")
        
    print(f"Загружено новых событий: {curr_cnt - prev_cnt}")
```
