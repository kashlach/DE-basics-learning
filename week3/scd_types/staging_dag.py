from airflow.providers.postgres.hook.postgres import PostgresHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

create_sql = '''
    CREATE SCHEMA IF NOT EXISTS source;

    CREATE TABLE IF NOT EXISTS source.customers (
        customer_id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        address VARCHAR(255),
        phone VARCHAR(50),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_deleted BOOLEAN DEFAULT FALSE);

    CREATE SCHEMA IF NOT EXISTS scd2;

    CREATE TABLE IF NOT EXISTS scd2.customers_staging (
        customer_id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        address VARCHAR(255),
        phone VARCHAR(50),
        source_updated_at TIMESTAMP,
        _loaded_at_ TIMESTAMP DEFAULT NOW(),
        _ddl_code_ VARCHAR(1) DEFAULT 'I');
        
    CREATE TABLE IF NOT EXISTS metadata (
        table_name VARCHAR(100) PRIMARY KEY,
        last_loaded_at TIMESTAMP,
        max_source_ts TIMESTAMP);
'''

merge_staging = '''
    MERGE INTO scd2.customers_staging AS st
    USING (
        SELECT 
            customer_id,
            name,
            address,
            phone,
            updated_at,
            is_deleted
        FROM source.customers 
        WHERE updated_at > %s
    ) AS s
    ON st.customer_id = s.customer_id
    WHEN MATCHED AND s.is_deleted = TRUE THEN 
        UPDATE SET
            source_updated_at = s.updated_at,
            _ddl_code_ = 'D',
            _loaded_at_ = %s
    WHEN MATCHED AND s.is_deleted = FALSE AND 
        (st.name IS DISTINCT FROM s.name OR
         st.address IS DISTINCT FROM s.address OR
         st.phone IS DISTINCT FROM s.phone) THEN
        UPDATE SET 
            name = s.name,
            address = s.address,
            phone = s.phone,
            source_updated_at = s.updated_at,
            _ddl_code_ = 'U',
            _loaded_at_ = %s
    WHEN MATCHED THEN 
        UPDATE SET _loaded_at_ = %s
    WHEN NOT MATCHED AND s.is_deleted = FALSE THEN
        INSERT (customer_id, name, address, phone, source_updated_at, _loaded_at_, _ddl_code_) 
        VALUES (s.customer_id, s.name, s.address, s.phone, s.updated_at, %s, 'I')
    WHEN NOT MATCHED AND s.is_deleted = TRUE THEN
        INSERT (customer_id, name, address, phone, source_updated_at, _loaded_at_, _ddl_code_) 
        VALUES (s.customer_id, s.name, s.address, s.phone, s.updated_at, %s, 'D')    
'''

metadata_upd = '''
    INSERT INTO scd2.metadata (table_name, last_loaded_at, max_source_ts)
    VALUES (%s, %s, %s)
    ON CONFLICT (table_name) DO UPDATE
    SET last_loaded_at = EXCLUDED.last_loaded_at,
        max_source_ts = EXCLUDED.max_source_ts;
'''

def init_schema():
    hook = PostgresHook(postgres_conn_id='github_events_db')
    hook.run(create_sql)
    logging.info("Требуемые схема и таблицы существуют")
    
def load_staging(**context):
    hook = PostgresHook(postgres_conn_id='github_events_db')
    table_name = 'customers_staging'
    
    # определяем дату, с которой будем искать изменения на источнике
    result = hook.get_first(
        "SELECT max_source_ts FROM scd2.metadata WHERE table_name = %s", 
        (table_name, )
    )
    
    if result and result[0]:
        max_ts = result[0]
        logging.info(f"Загружаем изменения с {max_ts}")
    else:
    # если данных в metadata нет, то первая загрузка => загружаем все
        max_ts = datetime(1900, 1, 1)
        logging.info("Первая загрузка, загружаем все")
        
    load_date = datetime.now()
    logging.info(f"Начало загрузки: {load_date}")
        
    # загружаем измененные данные в staging
    try:
        hook.run(merge_staging, parameters=(max_ts, load_date, load_date, load_date, load_date, load_date))
            logging.info("merge в staging выполнен")
    except Exception as e:
        logging.error(f"merge не выполнен: {e}")    
        raise
    
    # выбираем source_updated_at из загруженных записей
    result = hook.get_first(
        "SELECT MAX(source_updated_at) FROM scd2.customers_staging WHERE _loaded_at_ = %s", 
        (load_date, )
    )
    max_source_ts = result[0] if result else max_ts
    
    # обновляем metadata
    try:
        hook.run(metadata_upd, table_name, load_date, max_source_ts)
        logging.info(f"metadata обновлена: max_source_ts = {max_source_ts}")
    except Exception as e:
        logging.error(f"Ошибка при обновлении metadata: {e}")
        raise
    
    context['ti'].xcom_push(key='load_date', value=load_date.isoformat())
    context['ti'].xcom_push(key='max_source_ts', value=max_source_ts.isoformat())
    
def check_staging_state():
    hook = PostgresHook(postgres_conn_id='github_events_db')
    
    logging.info("customers_staging (последние 10 записей по source_updated_at):")
    rows = hook.get_records('''
        SELECT 
            customer_id,
            name,
            source_updated_at,
            _loaded_at_,
            _ddl_code_
        FROM scd2.customers_staging
        ORDER BY source_updated_at DESC
        LIMIT 10
    ''')
    for row in rows:
        logging.info(f"ID={row[0]}, Name={row[1]}, "
                    f"Source_Updated={row[2]}, Loaded={row[3]}, Oper={row[4]}")
    

    watermark = hook.get_first(
        "SELECT last_loaded_at, max_source_ts FROM scd2.metadata WHERE table_name = %s", 
        'customers_staging'
    )
    
    if watermark:
        logging.info(f"Watermark: last_load={watermark[0]}, max_ts={watermark[1]}")
        # посчитаем лаг от max_source_ts
        lag = datetime.now() - watermark[1]
        logging.info(f"Lag: {(lag.total_seconds() / 60):.1f} минут")
    else:
        logging.warning("Метаданные не найдены")
    

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
    'staging_customers_fill', 
    default_args=default_args,
    description='Инкрементальная загрузка staging для customers',
    schedule='*/5 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['first_dag', 'scd2', 'staging']
) as dag:

    init_task = PythonOperator(
        task_id='init_schema',
        python_callable=init_schema
    )
    
    load_data = PythonOperator(
        task_id='load_staging',
        python_callable=load_staging,
        provide_context=True
    )
    
    show_state = PythonOperator(
        task_id='check_staging_state',
        python_callable=check_staging_state
    ) 
    
init_task >> load_data >> show_state