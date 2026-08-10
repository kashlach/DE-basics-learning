'''
Сохранение данных в БД.
SQLite для тестов и разработки, PostgreSQL для продакшена.
'''
import json
import logging

from sqlalchemy import create_engine, text

from .config import DATABASE_URL

logger = logging.getLogger(__name__)

# engine один на все приложение
_engine = None

def get_engine():
    '''Возвращает engine (создаёт при первом вызове)'''
    global _engine
    if _engine is None:
        _engine = create_engine(DATABASE_URL)

    return _engine

def is_sqlite() -> bool:
    '''True если SQLite'''
    return "sqlite" in str(get_engine().url)

def ensure_tables() -> None:
    '''Проверка наличия таблиц (создаем, если надо)'''
    engine = get_engine()

    if is_sqlite():
        create_sql = '''
            CREATE TABLE IF NOT EXISTS currency_rates_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                loaded_at TEXT DEFAULT (datetime('now')),
                request_date TEXT NOT NULL UNIQUE,
                raw_json TEXT NOT NULL
            )
        '''
    else:
        create_sql = '''
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.currency_rates_raw (
                id SERIAL PRIMARY KEY,
                loaded_at TIMESTAMPTZ DEFAULT NOW(),
                request_date DATE NOT NULL UNIQUE,
                raw_json JSONB NOT NULL
            )
        '''

    # begin(), а не connect(): в SQLAlchemy 2.0 нет автокоммита,
    # без явной транзакции DDL откатится при закрытии соединения
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    logger.info("Таблица готова")

def upsert_data(request_date, raw_json) -> str:
    '''
    Вставляет или обновляет запись за дату.
    Возвращает "inserted" или "updated"
    '''
    ensure_tables()

    date_str = str(request_date)
    json_str = json.dumps(raw_json, ensure_ascii=False)

    if is_sqlite():
        return _upsert_sqlite(date_str, json_str)

    return _upsert_postgres(date_str, json_str)

def _upsert_postgres(date_str, json_str) -> str:
    '''
    Вставка и обновление одним запросом, опираясь на UNIQUE(request_date).
    Гонки между проверкой и записью нет: решает СУБД.

    xmax - служебный столбец: у только что вставленной строки он нулевой,
    у обновленной хранит id транзакции. Так отличаем insert от update.
    '''
    sql = text('''
        INSERT INTO raw.currency_rates_raw (request_date, raw_json)
        VALUES (CAST(:d AS DATE), CAST(:j AS JSONB))
        ON CONFLICT (request_date) DO UPDATE
        SET raw_json = EXCLUDED.raw_json, loaded_at = NOW()
        RETURNING (xmax = 0) AS inserted
    ''')

    with get_engine().begin() as conn:
        inserted = conn.execute(sql, {'d': date_str, 'j': json_str}).scalar()

    logger.info(f"{'Добавили' if inserted else 'Обновили'} {date_str}")
    return 'inserted' if inserted else 'updated'

def _upsert_sqlite(date_str, json_str) -> str:
    '''
    Проверка наличия, затем вставка или обновление.
    Для разработки: конкурентной записи тут не бывает
    '''
    with get_engine().begin() as conn:
        check = text('SELECT COUNT(*) FROM currency_rates_raw WHERE request_date = :d')
        exists = conn.execute(check, {'d': date_str}).scalar() > 0

        if exists:
            upd_sql = text("""
                UPDATE currency_rates_raw
                SET raw_json = :j, loaded_at = datetime('now')
                WHERE request_date = :d
            """)
            conn.execute(upd_sql, {"j": json_str, "d": date_str})
            logger.info(f"Обновили {date_str}")
            return 'updated'

        ins_sql = text("""
            INSERT INTO currency_rates_raw (request_date, raw_json)
            VALUES (:d, :j)
        """)
        conn.execute(ins_sql, {"d": date_str, "j": json_str})
        logger.info(f"Добавили {date_str}")
        return 'inserted'

def load_date(target_date) -> str:
    '''
    Сохраняет ответ из API.
    Возвращает "inserted", "updated" или "skipped"
    '''
    from .api_client import fetch_rates

    api_resp = fetch_rates(target_date)
    if api_resp is None:
        logger.info(f'Нет данных за {target_date}')
        return 'skipped'

    return upsert_data(target_date, api_resp)

def count_records(request_date=None):
    '''
    Возвращает к-во записей в таблице.
    С request_date - только за эту дату.
    '''
    ensure_tables()
    engine = get_engine()

    table_name = 'currency_rates_raw' if is_sqlite() else 'raw.currency_rates_raw'

    sql = f'SELECT COUNT(*) FROM {table_name}'
    params = {}
    if request_date is not None:
        sql += ' WHERE request_date = :d'
        params['d'] = str(request_date)

    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        total = result.scalar()
        logger.info(f"Записей в БД: {total}")
        return total
