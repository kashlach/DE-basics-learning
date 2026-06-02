from sqlalchemy import create_engine, text

engine = create_engine('postgresql://postgres:postgres@localhost:5432/github_events_db')
with engine.connect() as conn:
    res = conn.execute(text("SELECT count(*) FROM github_events"))
    for row in res:
        print(row)