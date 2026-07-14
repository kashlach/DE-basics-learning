CREATE USER grafanareader WITH PASSWORD 'grafana_pass';

GRANT USAGE ON SCHEMA finance_marts TO grafanareader;
GRANT USAGE ON SCHEMA finance_int TO grafanareader;

-- гранты перенесены в post-hook в dbt, т.к. таблицы пересоздаются DROP+CREATE
--GRANT SELECT ON ALL TABLES IN SCHEMA finance_marts TO grafanareader;
--GRANT SELECT ON ALL TABLES IN SCHEMA finance_int TO grafanareader;