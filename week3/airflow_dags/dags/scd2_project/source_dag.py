"""
Генератор тестовых данных для source.customers
Не требует установки faker - использует встроенную имитацию
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import random


# ==================== ВСТРОЕННАЯ ИМИТАЦИЯ FAKER ====================
class SimpleFaker:
    """
    Простая замена библиотеки Faker для генерации тестовых данных
    Не требует установки дополнительных пакетов
    """
    
    def __init__(self, locale='ru_RU'):
        self.locale = locale
        
        # Русские имена и фамилии
        self.first_names_male = [
            'Иван', 'Петр', 'Алексей', 'Дмитрий', 'Сергей', 'Александр', 
            'Михаил', 'Николай', 'Владимир', 'Андрей', 'Евгений', 'Олег',
            'Виктор', 'Василий', 'Константин', 'Павел', 'Степан', 'Федор'
        ]
        
        self.first_names_female = [
            'Мария', 'Ольга', 'Елена', 'Наталья', 'Анна', 'Татьяна',
            'Светлана', 'Ирина', 'Екатерина', 'Юлия', 'Людмила', 'Надежда',
            'Валентина', 'Галина', 'Алина', 'Вероника', 'Анастасия', 'Виктория'
        ]
        
        self.last_names = [
            'Петров', 'Иванов', 'Сидоров', 'Смирнов', 'Козлов', 'Васильев',
            'Новиков', 'Федоров', 'Морозов', 'Волков', 'Лебедев', 'Соколов',
            'Попов', 'Андреев', 'Макаров', 'Никитин', 'Захаров', 'Кузнецов'
        ]
        
        # Улицы
        self.streets = [
            'ул. Ленина', 'ул. Пушкина', 'ул. Гагарина', 'ул. Тверская',
            'ул. Садовая', 'ул. Мира', 'ул. Победы', 'ул. Свободы',
            'ул. Советская', 'ул. Кирова', 'ул. Горького', 'ул. Чехова',
            'ул. Толстого', 'ул. Достоевского', 'ул. Некрасова', 'ул. Есенина'
        ]
        
        # Города
        self.cities = [
            'Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург',
            'Нижний Новгород', 'Казань', 'Челябинск', 'Самара',
            'Омск', 'Ростов-на-Дону', 'Уфа', 'Красноярск',
            'Пермь', 'Воронеж', 'Волгоград', 'Краснодар'
        ]
        
        # Типы улиц
        self.street_types = ['ул.', 'пр.', 'пер.', 'б-р', 'наб.']
        
    def name(self):
        """Генерирует случайное русское имя"""
        is_male = random.choice([True, False])
        first_name = random.choice(self.first_names_male if is_male else self.first_names_female)
        last_name = random.choice(self.last_names)
        
        # Иногда добавляем отчество
        if random.choice([True, False]):
            patronymic = self._generate_patronymic(is_male)
            return f"{last_name} {first_name} {patronymic}"
        return f"{last_name} {first_name}"
    
    def _generate_patronymic(self, is_male):
        """Генерирует отчество"""
        prefixes = ['Иванович', 'Петрович', 'Сергеевич', 'Александрович', 'Владимирович',
                   'Дмитриевич', 'Алексеевич', 'Михайлович', 'Николаевич', 'Викторович']
        if not is_male:
            prefixes = [p.replace('ич', 'на') for p in prefixes]
        return random.choice(prefixes)
    
    def address(self):
        """Генерирует случайный российский адрес"""
        city = random.choice(self.cities)
        street_type = random.choice(self.street_types)
        street = random.choice(self.streets).replace('ул.', street_type)
        house = random.randint(1, 150)
        apartment = random.choice([None, random.randint(1, 200)])
        
        address = f"{city}, {street} {house}"
        if apartment:
            address += f", кв. {apartment}"
        return address
    
    def phone_number(self):
        """Генерирует российский номер телефона"""
        operator_codes = ['900', '901', '902', '903', '904', '905', '906', '909',
                         '910', '911', '912', '913', '914', '915', '916', '917', '918', '919',
                         '920', '921', '922', '923', '924', '925', '926', '927', '928', '929',
                         '930', '931', '932', '933', '934', '935', '936', '937', '938', '939',
                         '950', '951', '952', '953', '954', '955', '956', '957', '958', '959',
                         '960', '961', '962', '963', '964', '965', '966', '967', '968', '969']
        
        code = random.choice(operator_codes)
        part1 = random.randint(100, 999)
        part2 = random.randint(10, 99)
        part3 = random.randint(10, 99)
        
        formats = [
            f"+7-{code}-{part1}-{part2}{part3}",
            f"8-{code}-{part1}-{part2}{part3}",
            f"+7 ({code}) {part1}-{part2}{part3}",
            f"8 ({code}) {part1}-{part2}{part3}",
            f"+7{code}{part1}{part2}{part3}"
        ]
        return random.choice(formats)


# ==================== SQL ЗАПРОСЫ ====================
create_schema_sql = '''
    CREATE SCHEMA IF NOT EXISTS source;
    
    CREATE TABLE IF NOT EXISTS source.customers (
        customer_id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        address VARCHAR(255),
        phone VARCHAR(50),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_deleted BOOLEAN DEFAULT FALSE
    );
'''

generate_initial_sql = '''
    INSERT INTO source.customers (customer_id, name, address, phone, updated_at, is_deleted)
    SELECT customer_id, name, address, phone, updated_at, is_deleted
    FROM (
        VALUES 
            (1, 'Иван Петров', 'Москва, ул. Ленина 1', '+7-900-111-1111', %s, FALSE),
            (2, 'Мария Сидорова', 'Санкт-Петербург, ул. Пушкина 2', '+7-900-222-2222', %s, FALSE),
            (3, 'Петр Иванов', 'Новосибирск, ул. Гагарина 3', '+7-900-333-3333', %s, FALSE),
            (4, 'Ольга Смирнова', 'Екатеринбург, ул. Тверская 4', '+7-900-444-4444', %s, FALSE),
            (5, 'Алексей Козлов', 'Казань, ул. Садовая 5', '+7-900-555-5555', %s, FALSE)
    ) AS data(customer_id, name, address, phone, updated_at, is_deleted)
    WHERE NOT EXISTS (SELECT 1 FROM source.customers LIMIT 1);
'''


# ==================== ФУНКЦИИ DAG'а ====================
def init_schema(**context):
    """Создаёт схему и таблицу source.customers"""
    hook = PostgresHook(postgres_conn_id='github_events_db')
    
    try:
        hook.run(create_schema_sql)
        logging.info("✅ Схема source и таблица customers созданы/проверены")
    except Exception as e:
        logging.error(f"❌ Ошибка при создании схемы: {e}")
        raise


def generate_initial_data(**context):
    """Генерирует начальные данные в source.customers"""
    hook = PostgresHook(postgres_conn_id='github_events_db')
    now = datetime.now()
    
    try:
        hook.run(generate_initial_sql, parameters=(now, now, now, now, now))
        logging.info("✅ Начальные данные добавлены (если таблица была пуста)")
        
        result = hook.get_first("SELECT COUNT(*) FROM source.customers")
        if result:
            logging.info(f"📊 В таблице source.customers теперь {result[0]} записей")
    except Exception as e:
        logging.error(f"❌ Ошибка при генерации начальных данных: {e}")
        raise


def generate_changes(**context):
    """
    Генерирует случайные изменения в source.customers
    """
    hook = PostgresHook(postgres_conn_id='github_events_db')
    now = datetime.now()
    
    # Используем встроенный SimpleFaker
    fake = SimpleFaker('ru_RU')
    
    # Конфигурация вероятностей
    prob_no_changes = 0.15
    prob_new_customer = 0.30
    prob_update = 0.40
    prob_delete = 0.15
    
    # Решаем, будут ли вообще изменения
    no_changes = random.random() < prob_no_changes
    
    if no_changes:
        logging.info("🔵 НЕТ ИЗМЕНЕНИЙ: источник не обновлялся в этом цикле")
        context['ti'].xcom_push(key='changes_count', value=0)
        context['ti'].xcom_push(key='no_changes', value=True)
        return
    
    logging.info("🟢 ЕСТЬ ИЗМЕНЕНИЯ: начинаем генерацию")
    changes_count = 0
    
    # 1. Добавление новых клиентов
    if random.random() < prob_new_customer:
        result = hook.get_first("SELECT COALESCE(MAX(customer_id), 0) FROM source.customers")
        max_id = result[0] if result else 0
        
        new_count = random.randint(1, 3)
        for i in range(new_count):
            max_id += 1
            new_name = fake.name()
            new_address = fake.address()
            new_phone = fake.phone_number()
            
            hook.run("""
                INSERT INTO source.customers (customer_id, name, address, phone, updated_at, is_deleted)
                VALUES (%s, %s, %s, %s, %s, FALSE)
            """, parameters=(max_id, new_name, new_address, new_phone, now))
            logging.info(f"  ➕ Добавлен новый клиент: id={max_id}, имя={new_name[:25]}...")
            changes_count += 1
    else:
        logging.info("  ➖ Новые клиенты не добавлялись")
    
    # 2. Обновление существующих клиентов
    if random.random() < prob_update:
        result = hook.get_records("""
            SELECT customer_id FROM source.customers 
            WHERE is_deleted = FALSE 
            ORDER BY RANDOM() 
            LIMIT %s
        """, (random.randint(1, 2),))
        
        if result:
            for row in result:
                customer_id = row[0]
                change_type = random.choice(['name', 'address', 'phone', 'multiple'])
                
                if change_type == 'name':
                    new_name = fake.name()
                    hook.run("""
                        UPDATE source.customers 
                        SET name = %s, updated_at = %s 
                        WHERE customer_id = %s
                    """, parameters=(new_name, now, customer_id))
                    logging.info(f"  ✏️ Обновлено имя клиента {customer_id}: {new_name[:25]}...")
                    
                elif change_type == 'address':
                    new_address = fake.address()
                    hook.run("""
                        UPDATE source.customers 
                        SET address = %s, updated_at = %s 
                        WHERE customer_id = %s
                    """, parameters=(new_address, now, customer_id))
                    logging.info(f"  ✏️ Обновлён адрес клиента {customer_id}")
                    
                elif change_type == 'phone':
                    new_phone = fake.phone_number()
                    hook.run("""
                        UPDATE source.customers 
                        SET phone = %s, updated_at = %s 
                        WHERE customer_id = %s
                    """, parameters=(new_phone, now, customer_id))
                    logging.info(f"  ✏️ Обновлён телефон клиента {customer_id}")
                    
                else:  # multiple
                    hook.run("""
                        UPDATE source.customers 
                        SET name = %s, address = %s, updated_at = %s 
                        WHERE customer_id = %s
                    """, parameters=(fake.name(), fake.address(), now, customer_id))
                    logging.info(f"  ✏️ Обновлены имя и адрес клиента {customer_id}")
                    
                changes_count += 1
        else:
            logging.info("  ⚠️ Нет активных клиентов для обновления")
    else:
        logging.info("  ➖ Обновления клиентов не производились")
    
    # 3. Удаление клиентов
    if random.random() < prob_delete:
        result = hook.get_first("""
            SELECT customer_id FROM source.customers 
            WHERE is_deleted = FALSE AND customer_id > 5
            ORDER BY RANDOM() LIMIT 1
        """)
        
        if result:
            customer_id = result[0]
            hook.run("""
                UPDATE source.customers 
                SET is_deleted = TRUE, updated_at = %s 
                WHERE customer_id = %s
            """, parameters=(now, customer_id))
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
    
    context['ti'].xcom_push(key='changes_count', value=changes_count)
    context['ti'].xcom_push(key='no_changes', value=no_changes)


def show_source_state(**context):
    """Показывает текущее состояние источника"""
    hook = PostgresHook(postgres_conn_id='github_events_db')
    
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
        logging.info(f"📊 Статистика: всего={stats[0]}, удалено={stats[1]}, изменено за час={stats[2]}")
        
        last_update = stats[3]
        if last_update:
            lag_minutes = (datetime.now() - last_update).total_seconds() / 60
            if lag_minutes > 10:
                logging.warning(f"⚠️ Источник не обновлялся {lag_minutes:.0f} минут!")
            else:
                logging.info(f"✅ Лаг источника: {lag_minutes:.1f} минут")
    
    logging.info("=" * 80)


# ==================== НАСТРОЙКА DAG ====================
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
    
    init_schema_task = PythonOperator(
        task_id='init_schema',
        python_callable=init_schema,
        provide_context=True,
    )
    
    init_data = PythonOperator(
        task_id='generate_initial_data',
        python_callable=generate_initial_data,
        provide_context=True,
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
    
    init_schema_task >> init_data >> generate_changes_task >> show_state