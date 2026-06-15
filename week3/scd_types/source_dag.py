from airflow.providers.postgres.hook.postgres import PostgresHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import random
from faker import Faker

fake = Faker('ru_RU')

# Конфигурация вероятностей (можно менять)
CONFIG = {
    'prob_no_changes': 0.15,      # 15% что вообще не будет изменений
    'prob_new_customer': 0.30,    # 30% что добавится 1 клиент
    'prob_update': 0.40,          # 40% что будет обновление
    'prob_delete': 0.15,          # 15% что будет удаление
}

generate_initial_sql = '''
    INSERT INTO source.customers (customer_id, name, address, phone, updated_at, is_deleted)
    SELECT customer_id, name, address, phone, updated_at, is_deleted
    FROM (
        VALUES 
            (1, 'Иван Петров', 'ул. Ленина 1', '+7-900-111-1111', %s, FALSE),
            (2, 'Мария Сидорова', 'ул. Пушкина 2', '+7-900-222-2222', %s, FALSE),
            (3, 'Петр Иванов', 'ул. Гагарина 3', '+7-900-333-3333', %s, FALSE),
            (4, 'Ольга Смирнова', 'ул. Тверская 4', '+7-900-444-4444', %s, FALSE),
            (5, 'Алексей Козлов', 'ул. Садовая 5', '+7-900-555-5555', %s, FALSE)
    ) AS data(customer_id, name, address, phone, updated_at, is_deleted)
    WHERE NOT EXISTS (SELECT 1 FROM source.customers LIMIT 1);
'''

def generate_initial_data(**context):
    """Генерирует начальные данные в source.customers"""
    hook = PostgresHook(postgres_conn_id='github_events_db')
    now = datetime.now()
    
    try:
        hook.run(generate_initial_sql, parameters=(now, now, now, now, now))
        logging.info("Начальные данные добавлены (если таблица была пуста)")
        
        result = hook.get_first("SELECT COUNT(*) FROM source.customers")
        if result:
            logging.info(f"В таблице source.customers теперь {result[0]} записей")
    except Exception as e:
        logging.error(f"Ошибка при генерации начальных данных: {e}")
        raise

def generate_changes(**context):
    """
    Генерирует случайные изменения в source.customers.
    Теперь явно обрабатывает сценарий "нет изменений".
    """
    hook = PostgresHook(postgres_conn_id='github_events_db')
    now = datetime.now()
    
    # Решаем, будут ли вообще изменения в этом цикле
    no_changes = random.random() < CONFIG['prob_no_changes']
    
    if no_changes:
        logging.info("🔵 НЕТ ИЗМЕНЕНИЙ: источник не обновлялся в этом цикле")
        # Всё равно обновляем метаданные (heartbeat)
        return
    
    logging.info("🟢 ЕСТЬ ИЗМЕНЕНИЯ: начинаем генерацию")
    
    changes_count = 0
    
    # 1. Добавление новых клиентов (с вероятностью)
    if random.random() < CONFIG['prob_new_customer']:
        result = hook.get_first("SELECT COALESCE(MAX(customer_id), 0) FROM source.customers")
        max_id = result[0] if result else 0
        
        # Добавляем от 1 до 3 новых клиентов
        new_count = random.randint(1, 3)
        for i in range(new_count):
            max_id += 1
            hook.run("""
                INSERT INTO source.customers (customer_id, name, address, phone, updated_at, is_deleted)
                VALUES (%s, %s, %s, %s, %s, FALSE)
            """, (max_id, fake.name(), fake.address(), fake.phone_number(), now))
            logging.info(f"  ➕ Добавлен новый клиент: id={max_id}")
            changes_count += 1
    else:
        logging.info("  ➖ Новые клиенты не добавлялись")
    
    # 2. Обновление существующих клиентов (с вероятностью)
    if random.random() < CONFIG['prob_update']:
        # Получаем список активных клиентов (не удалённых)
        result = hook.get_records("""
            SELECT customer_id, name, address, phone 
            FROM source.customers 
            WHERE is_deleted = FALSE 
            ORDER BY RANDOM() 
            LIMIT %s
        """, (random.randint(1, 2),))
        
        if result:
            for row in result:
                customer_id = row[0]
                # Выбираем тип изменения
                change_type = random.choice(['name', 'address', 'phone', 'multiple'])
                
                if change_type == 'name':
                    new_name = fake.name()
                    hook.run("""
                        UPDATE source.customers 
                        SET name = %s, updated_at = %s 
                        WHERE customer_id = %s
                    """, (new_name, now, customer_id))
                    logging.info(f"  ✏️ Обновлено имя клиента {customer_id}: {new_name}")
                    
                elif change_type == 'address':
                    new_address = fake.address()
                    hook.run("""
                        UPDATE source.customers 
                        SET address = %s, updated_at = %s 
                        WHERE customer_id = %s
                    """, (new_address, now, customer_id))
                    logging.info(f"  ✏️ Обновлён адрес клиента {customer_id}")
                    
                elif change_type == 'phone':
                    new_phone = fake.phone_number()
                    hook.run("""
                        UPDATE source.customers 
                        SET phone = %s, updated_at = %s 
                        WHERE customer_id = %s
                    """, (new_phone, now, customer_id))
                    logging.info(f"  ✏️ Обновлён телефон клиента {customer_id}")
                    
                else:  # multiple
                    hook.run("""
                        UPDATE source.customers 
                        SET name = %s, address = %s, updated_at = %s 
                        WHERE customer_id = %s
                    """, (fake.name(), fake.address(), now, customer_id))
                    logging.info(f"  ✏️ Обновлены имя и адрес клиента {customer_id}")
                    
                changes_count += 1
        else:
            logging.info("  ⚠️ Нет активных клиентов для обновления")
    else:
        logging.info("  ➖ Обновления клиентов не производились")
    
    # 3. Удаление клиентов (с вероятностью)
    if random.random() < CONFIG['prob_delete']:
        result = hook.get_first("""
            SELECT customer_id FROM source.customers 
            WHERE is_deleted = FALSE AND customer_id > 5  -- не удаляем начальных
            ORDER BY RANDOM() LIMIT 1
        """)
        
        if result:
            customer_id = result[0]
            hook.run("""
                UPDATE source.customers 
                SET is_deleted = TRUE, updated_at = %s 
                WHERE customer_id = %s
            """, (now, customer_id))
            logging.info(f"  🗑️ Клиент {customer_id} помечен как удалённый")
            changes_count += 1
        else:
            logging.info("  ⚠️ Нет подходящих клиентов для удаления")
    else:
        logging.info("  ➖ Удаления клиентов не производились")
    
    # Логируем итоги
    if changes_count == 0:
        logging.info("🔵 ИЗМЕНЕНИЙ НЕТ (несмотря на попытки, ничего не изменилось)")
    else:
        logging.info(f"✅ Сгенерировано {changes_count} изменений")
    
    # Сохраняем количество изменений в XCom для мониторинга
    context['ti'].xcom_push(key='changes_count', value=changes_count)
    context['ti'].xcom_push(key='no_changes', value=no_changes)

def show_source_state(**context):
    """Показывает текущее состояние источника с акцентом на изменения"""
    hook = PostgresHook(postgres_conn_id='github_events_db')
    
    # Получаем информацию об изменениях из XCom
    changes_count = context['ti'].xcom_pull(key='changes_count', task_ids='generate_changes')
    no_changes = context['ti'].xcom_pull(key='no_changes', task_ids='generate_changes')
    
    logging.info("=" * 80)
    
    if no_changes:
        logging.info("🔵 СОСТОЯНИЕ ИСТОЧНИКА (без изменений):")
    else:
        logging.info(f"🟢 СОСТОЯНИЕ ИСТОЧНИКА (изменений: {changes_count}):")
    
    logging.info("-" * 80)
    
    rows = hook.get_records("""
        SELECT customer_id, name, address, phone, updated_at, is_deleted
        FROM source.customers
        ORDER BY customer_id
        LIMIT 15
    """)
    
    for row in rows:
        status = "🗑️ DELETED" if row[5] else "✅ ACTIVE"
        # Сокращаем длинные поля для читаемости
        name_short = row[1][:25] + "..." if len(row[1]) > 25 else row[1]
        logging.info(f"ID={row[0]:3d} | {name_short:<28} | {row[4].strftime('%H:%M:%S')} | {status}")
    
    # Статистика
    stats = hook.get_first("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN is_deleted THEN 1 ELSE 0 END) as deleted,
            COUNT(CASE WHEN updated_at > NOW() - INTERVAL '1 hour' THEN 1 END) as last_hour,
            MAX(updated_at) as last_update
        FROM source.customers
    """)
    
    if stats:
        logging.info("-" * 80)
        logging.info(f"📊 Статистика: всего={stats[0]}, удалено={stats[1]}, "
                    f"изменено за час={stats[2]}")
        logging.info(f"⏰ Последнее обновление: {stats[3]}")
        
        # Расчёт лага
        last_update = stats[3]
        if last_update:
            lag_minutes = (datetime.now() - last_update).total_seconds() / 60
            if lag_minutes > 10:
                logging.warning(f"⚠️ Источник не обновлялся {lag_minutes:.0f} минут!")
            else:
                logging.info(f"✅ Лаг источника: {lag_minutes:.1f} минут")
    
    logging.info("=" * 80)

def advanced_check(**context):
    """
    Продвинутая проверка: показывает, какие сценарии были протестированы
    """
    changes_count = context['ti'].xcom_pull(key='changes_count', task_ids='generate_changes')
    no_changes = context['ti'].xcom_pull(key='no_changes', task_ids='generate_changes')
    
    logging.info("=" * 80)
    logging.info("📋 ОТЧЁТ О ТЕСТИРОВАНИИ:")
    logging.info(f"  - Сценарий 'нет изменений': {'✅ ПРОТЕСТИРОВАН' if no_changes else '⏳ Ещё не выпал'}")
    logging.info(f"  - Количество изменений в этом цикле: {changes_count}")
    
    # Статистика по запускам (можно хранить в XCom или БД)
    # Здесь просто для демонстрации
    if no_changes:
        logging.info("  💡 Совет: проверьте, как staging DAG обрабатывает холостые запуски")
    
    logging.info("=" * 80)

# Настройка DAG
default_args = {
    'owner': 'pypypy',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'customer_source_generator',
    default_args=default_args,
    description='Генератор данных для source.customers (имитация микросервиса)',
    schedule='*/3 * * * *',  # Каждые 3 минуты
    catchup=False,
    max_active_runs=1,
    tags=['generator', 'source', 'test-data'],
) as dag:
    
    init_data = PythonOperator(
        task_id='generate_initial_data',
        python_callable=generate_initial_data,
    )
    
    generate_changes_task = PythonOperator(
        task_id='generate_changes',
        python_callable=generate_changes,
        provide_context=True,
    )
    
    show_state = PythonOperator(
        task_id='show_source_state',
        python_callable=show_source_state,
        provide_context=True,
    )
    
    advanced_check_task = PythonOperator(
        task_id='advanced_check',
        python_callable=advanced_check,
        provide_context=True,
    )
    
    init_data >> generate_changes_task >> show_state >> advanced_check_task