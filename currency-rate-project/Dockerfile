FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
RUN pip install dbt-postgres