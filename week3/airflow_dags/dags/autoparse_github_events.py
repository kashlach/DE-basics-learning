from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

doc = '''
    ## Автоматический запуск ранее написанного парсера
    Запускает парсер каждые 5 минут и проверяет, что данные загрузились.
    
    ### Что делает:
    1. Создает таблицу для логирования при необходимости
    2. Проверяет количество запусков (не более 10)
    3. Запоминает текущее количество строк
    4. Запускает парсер
    5. Проверяет, что количество строк увеличилось
    6. Логирует
'''

create_table_sql = '''
    CREATE TABLE IF NOT EXISTS dag_execution_log (
        dag_name VARCHAR(50),
        run_id VARCHAR(50) UNIQUE,
        execution_date TIMESTAMP
    );
'''

select_rowcount_sql = "SELECT COUNT(*) FROM github_events"

insert_data_sql = '''
    INSERT INTO dag_execution_log (dag_name, run_id, execution_date)
    VALUES (%s, %s, %s)
'''

def create_log_table():
    hook = PostgresHook(postgres_conn_id='github_events_db')
    hook.run(create_table_sql)

def check_exec_limit(**context):
    max_cnt = 10
    var_name = 'autoparse_github_runs'
    
    current_runs = int(Variable.get(var_name, default_var=0))
    
    if current_runs > max_cnt:
        return 'end'
      
    Variable.set(var_name, current_runs + 1)
    return 'get_initial_count'
    
def get_rowcount():
    hook = PostgresHook(postgres_conn_id='github_events_db')
    result = hook.get_first(select_rowcount_sql)
    return result[0]
    
def check_data_arrived(**context):
    hook = PostgresHook(postgres_conn_id='github_events_db')
    
    prev_cnt = context['ti'].xcom_pull(task_ids='get_initial_count')
    curr_cnt = hook.get_first(select_rowcount_sql)
    curr_cnt = curr_cnt[0]
    
    if prev_cnt == curr_cnt:
        raise Exception("Нет новых событий за последние 5 минут")
        
    print(f"Загружено новых событий: {curr_cnt - prev_cnt}")
    
def log_exec_status(**context):  
    hook = PostgresHook(postgres_conn_id='github_events_db')
    hook.run(insert_data_sql, parameters=('autoparse_github_events', context['run_id'], context['data_interval_start']))
    
default_args = {
    'owner': 'pypypy',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'autoparse_github_events',
    doc_md=doc, 
    default_args=default_args,
    description='Автоматический запуск существующего скрипта',
    schedule='*/5 * * * *',
    catchup=False,
    tags=['github_events', 'first_dag']
) as dag: 

    log_init = PythonOperator(
        task_id='init_log_table',
        python_callable=create_log_table
    )
    
    check_limit = BranchPythonOperator(
        task_id='check_exec_limit',
        python_callable=check_exec_limit,
        provide_context=True
    )

    get_initial_count = PythonOperator(
        task_id='get_initial_count',
        python_callable=get_rowcount
    )

    run_script = BashOperator(
        task_id='run_github_loader',
        bash_command='python /opt/airflow/dags/parser.py',
        execution_timeout=timedelta(minutes=1)
    )

    verify = PythonOperator(
        task_id='verify_new_data',
        python_callable=check_data_arrived,
        provide_context=True
    )

    log_exec = PythonOperator(
        task_id='log_exec_status',
        python_callable=log_exec_status,
    provide_context=True
    )
    
    task_end = DummyOperator(task_id='end', trigger_rule='one_success')

log_init >> check_limit 
check_limit >> [get_initial_count, task_end]
get_initial_count >> run_script >> verify >> log_exec >> task_end  


    


