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

    with engine.connect() as conn:
        conn.execute(text(create_sql))

    logger.info("Таблица готова")

def upsert_data(request_date, raw_json) -> str:
    '''
    Вставляет или обновляет запись за дату.
    Возвращает "inserted" или "updated"
    '''
    ensure_tables()
    engine = get_engine()

    date_str = str(request_date)
    json_str = json.dumps(raw_json, ensure_ascii=False)
    table_name = 'currency_rates_raw' if is_sqlite() else 'raw.currency_rates_raw'

    with engine.connect() as conn:
        check = text(f'SELECT COUNT(*) FROM {table_name} WHERE request_date = :d')
        result = conn.execute(check, {'d': date_str})
        exists = result.scalar() > 0

        if exists:
            if is_sqlite():
                upd_sql = text(f"""
                    UPDATE {table_name}
                    SET raw_json = :j, loaded_at = datetime('now')
                    WHERE request_date = :d
                """)
            else:
                upd_sql = text(f"""
                    UPDATE {table_name}
                    SET raw_json = CAST(:j AS JSONB), loaded_at = NOW()
                    WHERE request_date = :d
                """)
            conn.execute(upd_sql, {"j": json_str, "d": date_str})
            logger.info(f"Обновили {date_str}")
            return 'updated'
        else:
            ins_sql = text(f"""
                INSERT INTO {table_name} (request_date, raw_json)
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

def count_records():
    '''Возвращает к-во записей в таблице'''
    ensure_tables()
    engine = get_engine()

    table_name = 'currency_rates_raw' if is_sqlite() else 'raw.currency_rates_raw'

    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM {table_name}'))
        total = result.scalar()
        logger.info(f"Записей в БД: {total}")
        return total
