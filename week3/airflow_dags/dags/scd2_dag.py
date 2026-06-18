from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
import logging

create_sql = '''
    CREATE SCHEMA IF NOT EXISTS scd2;
    
    CREATE SEQUENCE IF NOT EXISTS scd2.surrogate_key
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    NO MAXVALUE;
    
    CREATE TABLE IF NOT EXISTS scd2.dim_customers(
        id INTEGER PRIMARY KEY DEFAULT nextval('scd2.surrogate_key'),
        customer_id INTEGER,
        name VARCHAR(100),
        address VARCHAR(255),
        phone VARCHAR(50),
        source_updated_at TIMESTAMP,
        created_timestamp TIMESTAMP DEFAULT NOW(),
        is_active BOOLEAN DEFAULT TRUE
    );
'''

merge_dim_sql = '''
    UPDATE scd2.dim_customers AS tt
    SET is_active = False
    FROM (
        SELECT customer_id
        FROM scd2.customers_staging st
        WHERE st._loaded_at_ = %s 
          AND st._ddl_code_ IN ('U', 'D') 
    ) AS st
    WHERE tt.customer_id = st.customer_id
      AND tt.is_active = True;
    
    INSERT INTO scd2.dim_customers 
       (customer_id,
        name,
        address,
        phone,
        source_updated_at,
        created_timestamp,
        is_active)
    SELECT
        customer_id,
        name,
        address,
        phone,
        source_updated_at,
        NOW(),
        True
    FROM scd2.customers_staging st
    WHERE _loaded_at_ = %s 
      AND st._ddl_code_ IN ('I', 'U');
'''

metadata_upd = '''
    INSERT INTO scd2.metadata (table_name, last_loaded_at, max_source_ts)
    VALUES (%s, %s, %s)
    ON CONFLICT (table_name) DO UPDATE
    SET last_loaded_at = EXCLUDED.last_loaded_at, 
        max_source_ts = EXCLUDED.max_source_ts
'''

def init_sql():
    hook = PostgresHook(postgres_conn_id='github_events_db')
    hook.run(create_sql)
    logging.info('Требуемые схема и таблицы существуют')
    
def dim_load(**context):
    hook = PostgresHook(postgres_conn_id='github_events_db')
    table_name = 'dim_customers'
    
    # получаем дату последней загрузки в staging
    staging_last_load = context['ti'].xcom_pull(
        key='load_date', 
        dag_id='staging_customers_fill', 
        task_ids='load_staging',
        include_prior_dates=True
    )
    
    if not staging_last_load:
        logging.warning("Нет данных о последней загрузке staging")
        return
        
    logging.info(f"Обрабатываем staging данные от {staging_last_load}")
    staging_last_load = datetime.fromisoformat(staging_last_load)
    try:
        hook.run(merge_dim_sql, parameters=(staging_last_load, staging_last_load))
        logging.info("scd2 обновление выполнено")
    except Exception as e:
        logging.error(f"scd2 обновление не выполнено: {e}")
        raise
    
    # выбираем source_updated_at из загруженных записей или metadata
    result = hook.get_first('''
        SELECT 
            COALESCE(
                MAX(source_updated_at),
                (SELECT max_source_ts FROM scd2.metadata WHERE table_name = %s),
                DATE'1900-01-01'::TIMESTAMP     
                    )
        FROM scd2.dim_customers WHERE created_timestamp >= %s
        ''', 
        parameters=(table_name, datetime.now() - timedelta(hours=1)) # за последний час, тк не каждую итерацию будут изменения на источнике
    )
    max_source_ts = result[0] if result and result[0] else datetime(1900, 1, 1)
    load_date = datetime.now()
    
    # обновляем metadata
    try:
        hook.run(metadata_upd, parameters=(table_name, load_date, max_source_ts))
        logging.info("metadata обновлена")
    except Exception as e:
        logging.error("metadata не обновлена: {e}")
        raise
    
    # смотрим отставание dim от staging
    staging_max_ts = context['ti'].xcom_pull(
        key='max_source_ts', 
        dag_id='staging_customers_fill', 
        task_ids='load_staging',
        include_prior_dates=True
    )
    if staging_max_ts and max_source_ts:
        staging_max_ts = datetime.fromisoformat(staging_max_ts)
        lag = staging_max_ts - max_source_ts
        logging.info(f"Лаг dim от staging: {(lag.total_seconds() / 60):.1f} минут")
    
def show_sample_data():
    hook = PostgresHook(postgres_conn_id='github_events_db')
    
    logging.info("dim_customers (последние 10 записей по id):")
    rows = hook.get_records('''
        SELECT 
            id, 
            customer_id, 
            name, 
            source_updated_at, 
            created_timestamp, 
            is_active
        FROM scd2.dim_customers
        ORDER BY id DESC
        LIMIT 10
    ''')
    
    for row in rows:
        logging.info(f"ID={row[0]}, Customer={row[1]}, Name={row[2]}, "
                    f"Source_Updated={row[3]}, Created={row[4]}, Is_active={row[5]}")
        
    stats = hook.get_first('''
        SELECT 
            COUNT(*) AS total,
            COUNT(DISTINCT customer_id) AS unique,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active
        FROM scd2.dim_customers
    ''')
    
    if stats:
        logging.info(f"Статистика: {stats[1]} всего версий,"
                    f"{stats[0]} уникальных клиентов,  {stats[2]} активных")
                    
    logging.info("Проверим наличие нескольких is_active при одном customer_id:")
    rows = hook.get_records('''
        SELECT  
            customer_id, 
            STRING_AGG(id::text, ' ,'::text ORDER BY id DESC) ids,
            SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) active_versions
        FROM scd2.dim_customers
        GROUP BY customer_id
        HAVING SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) > 1
        ORDER BY customer_id
    ''')
    
    if rows:
        for row in rows:
            logging.warning(f"Сustomer {row[0]}: {row[2]} is_active у id {row[1]}")
    else:
        logging.info("Все хорошо")
    
default_args = {
    'owner': 'pypypy',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'dim_customers_fill', 
    default_args=default_args,
    description='SCD Type 2 загрузка dim_customers из staging',
    schedule='*/8 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['first_dag', 'scd2', 'dimension']
) as dag:
    init_task = PythonOperator(
        task_id='init_schema',
        python_callable=init_sql
    )
    
    load_dim = PythonOperator(
        task_id='load_dim',
        python_callable=dim_load,
        provide_context=True
    )
    
    show_state = PythonOperator(
        task_id='show_state',
        python_callable=show_sample_data
    )

init_task >> load_dim >> show_state