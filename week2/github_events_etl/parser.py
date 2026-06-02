from sqlalchemy import create_engine, text
import requests
import pandas as pd
from sqlalchemy.dialects.postgresql import JSONB

engine = create_engine('postgresql://postgres:postgres@localhost:5432/github_events_db')

url = 'https://api.github.com/events'
headers = {'Accept': 'application/vnd.github.v3+json'}

response = requests.get(url, headers=headers)
data = response.json()
print(f'\nПолучено событий по API: {len(data)}')

events_list = []
events_raw_list = []

for event in data:
    events_list.append({
        'event_id': event['id'],
        'type': event['type'],
        'repo': event['repo']['name'],
        'actor': event['actor']['login'],
        'created_at': event['created_at']
    })

    events_raw_list.append({
        'event_id': event['id'],
        'raw_json': event
    })

df_events = pd.DataFrame(events_list)
df_raw = pd.DataFrame(events_raw_list)

df_events = df_events.drop_duplicates(subset=['event_id'])
df_raw = df_raw.drop_duplicates(subset=['event_id'])

print(f'Добавлено новых событий в БД: {len(df_events)}')

df_events.to_sql(
    name='github_events', 
    con=engine, 
    if_exists='append', 
    index=False
)
df_raw.to_sql(
    name='github_events_raw', 
    con=engine, 
    if_exists='append',
    index=False,
    dtype={'raw_json': JSONB}
)
with engine.connect() as conn:
    result = conn.execute(text('''
        SELECT COUNT(*)
        FROM github_events
    ''')).fetchone()
    print(f'\nВсего записей в таблице github_events: {result[0]}')

    print('\nИнфа по этим данным:')
    print('\n_____Топ-5 активных репозиториев_____')
    result = conn.execute(text('''
        SELECT
            repo,
            COUNT(event_id) AS event_cnt,
            COUNT(DISTINCT actor) AS unique_actor
        FROM github_events
        GROUP BY repo
        ORDER BY event_cnt DESC
        LIMIT 5
    '''))
    for row in result:
        print(f'{row[0]}: {row[1]} событий, {row[2]} участников')

    print('\n_____Типы событий_____')
    result = conn.execute(text('''
        SELECT
            type,
            COUNT(event_id) AS event_cnt,
            COUNT(DISTINCT actor) AS unique_actor
        FROM github_events
        GROUP BY type
        ORDER BY event_cnt DESC
        LIMIT 5
    '''))
    for row in result:
        print(f'{row[0]}: {row[1]} событий')

    print('\n_____login != display_login (данные из JSONB)_____')
    result = conn.execute(text('''
        WITH actor_info AS (
            SELECT
                raw_json -> 'actor' ->> 'login' AS login,
                raw_json -> 'actor' ->> 'display_login' AS display_login
            FROM github_events_raw
        )
        SELECT
            DISTINCT login, display_login                           
        FROM actor_info
        WHERE  login <> display_login
        ORDER BY login
        LIMIT 10
    '''))
    for row in result:
        print(f'login "{row[0]}", а отображаемый - "{row[1]}"')

print('\nГотово!')