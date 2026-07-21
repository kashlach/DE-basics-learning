import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.append('/opt/airflow/src')

from elt.loader import count_records, ensure_tables, get_engine, load_date

email_to = os.getenv("REPORT_MAIL_TO")

def ensure_raw_schema():
    get_engine()
    ensure_tables()

def extract_and_load(**context):
    target_date = datetime.fromisoformat(context['ds'])
    # context['ds'] - логич. дата, не дата факт. выпол-я
    # = вчера при автоматич запуске
    # = сегодня при ручном
    load_date(target_date)

def check_data_loaded():
    total = count_records()
    if total == 0:
        raise Exception('В БД нет записей после загрузки!')

def form_weekly_rep(**context):
    '''
    Эксель отчет с данными по курсам определенных валют за неделю.
    Отчет по пн при запуске по расписанию и в любой день при ручном.
    Возвращает путь к файлу, если отчет сформирован автоматом.
    '''
    logical_date = context['ds'] # str YYYY-MM-DD
    is_manual = context['dag_run'].external_trigger

    # при авт запуске logical_date в пн = вс
    # => чтобы отчет сформ в пн, нужно проверять на вс
    # при ручном logical_date в пн = пн
    if datetime.fromisoformat(logical_date).isoweekday() != 7 and not is_manual:
        logging.info('Не Пн, отчет не формируем')
        return

    logging.info('Формируем еженедельный отчет')
    import pandas as pd
    from sqlalchemy import text

    engine = get_engine()
    df = pd.read_sql(text('''
        SELECT *, EXTRACT(WEEK FROM requested_date) AS week_num
        FROM finance_marts.dm_weekly_report
        ORDER BY requested_date DESC, char_code ASC
    '''),
    engine)

    if is_manual:
        rep_name_end = logical_date + '_manual'
    else:
        rep_name_end = logical_date

    # - ./airflow/reports:/opt/airflow/reports/
    write_path = f'/opt/airflow/reports/weekly_report_{rep_name_end}.xlsx'
    with pd.ExcelWriter(write_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='detailed')

    return write_path if not is_manual else None # будет в xcom таски как return_value

def send_weekly_rep(**context):

    rep_path = context['ti'].xcom_pull(task_ids='form_rep')

    if not rep_path:
        logging.info('Отсутствует отчет к отправке')
        return

    from airflow.providers.smtp.hooks.smtp import SmtpHook
    smtp_hook = SmtpHook(smtp_conn_id='smtp_default')

    logging.info('Отправляем еженедельный отчет')
    with smtp_hook as hook:
        hook.send_email_smtp(
            to=[email_to],
            subject='Еженедельный отчет по курсам валют',
            html_content='<p>См. отчет во вложении</p>',
            files=[rep_path]
        )

def run_usd_alert(**context):
    from sqlalchemy import text

    check_date = context['ds']

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text('''
            SELECT current_rate, prev_date_rate, daily_change_pct
            FROM finance_marts.dm_change_alerts
            WHERE requested_date = CAST(:dt AS DATE)
              AND alert_triggered = TRUE
        '''), {'dt': check_date}).fetchone()

        if not (row and row[0]):
            logging.info('С курсом USD все нормально')
            return

        logging.info('Скачок курса USD!')
        logging.info(f'Был {row[0]}, стал {row[1]} -> изменение на {row[2]}%')

        change_dir = 'Повышение' if row[2] >= 0 else 'Снижение'

    # отправка алерта
    from airflow.providers.smtp.hooks.smtp import SmtpHook
    smtp_hook = SmtpHook(smtp_conn_id='smtp_default')

    subject = 'Скачок курса USD!'
    body = f'''
    <h1>{change_dir} курса</h1>
    <ul>
        <li><b>Предыдущее значение:</b> {row[1]}</li>
        <li><b>Текущее значение:</b> {row[0]}</li>
        <li><b>Изменение:</b> {row[2]}%</li>
    </ul>
    '''

    with smtp_hook as hook:
        hook.send_email_smtp(
            to=[email_to],
            subject=subject,
            html_content=body
        )


def_args = {
    'owner': 'pypipy',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'currency_pipeline',
    default_args=def_args,
    description='Ежедневная загрузка курсов валют ЦБ РФ',
    schedule='0 10 * * *', # 10:00 UTC, т.е. 13 по МСК
    catchup=False,
    max_active_runs=1,
    tags=['raw', 'currency']
) as dag:

    # загрузка данных в БД raw слой
    # extract
    prepare = PythonOperator(task_id='prepare_db', python_callable=ensure_raw_schema)
    load = PythonOperator(task_id='load_rates', python_callable=extract_and_load)
    check = PythonOperator(task_id='check_results', python_callable=check_data_loaded)

    # dbt обработка ответа
    # transform и load
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/currency_dbt && dbt run --target prod'
    )

    # тестируем загруженные данные
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/currency_dbt && dbt test --target prod'
    )

    # формируем excel отчет и алерт
    form_rep = PythonOperator(task_id='form_rep', python_callable=form_weekly_rep)
    alert = PythonOperator(task_id='usd_alert', python_callable=run_usd_alert)

    # отправляем excel отчет
    send_rep = PythonOperator(
        task_id='send_rep',
        python_callable=send_weekly_rep
    )


prepare >> load >> check >> dbt_run >> dbt_test >> [form_rep, alert]
form_rep >> send_rep
