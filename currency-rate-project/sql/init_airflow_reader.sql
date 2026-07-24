CREATE USER airflowreader WITH PASSWORD 'afreader_pass';
GRANT CONNECT ON DATABASE airflow TO airflowreader;
GRANT USAGE ON SCHEMA public TO airflowreader;
GRANT SELECT ON TABLE public.dag_run TO airflowreader;
GRANT SELECT ON TABLE public.task_fail TO airflowreader;