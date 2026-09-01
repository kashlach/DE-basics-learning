SET custom.bi_reader_pwd = :'bi_pwd'; -- переносим переменную psql в сессию самой БД

DO $$
DECLARE
  pwd TEXT;
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bireader') THEN
	  pwd := current_setting('custom.bi_reader_pwd'); -- считываем из переменной сессии
      EXECUTE format('CREATE ROLE bireader LOGIN PASSWORD %L', pwd);
   END IF;
END$$;

RESET custom.bi_reader_pwd;

GRANT USAGE ON SCHEMA finance_marts TO bireader;
GRANT USAGE ON SCHEMA finance_int TO bireader;
GRANT USAGE ON SCHEMA raw TO bireader;

GRANT SELECT ON raw.currency_rates_raw TO bireader;
GRANT SELECT ON ALL TABLES IN SCHEMA finance_marts TO bireader;
GRANT SELECT ON ALL TABLES IN SCHEMA finance_int TO bireader;

SELECT 'CREATE DATABASE superset;'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec
