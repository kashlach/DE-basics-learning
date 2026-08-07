import logging
import os
import sys
from datetime import date, datetime, timedelta

import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.append('/opt/airflow/src')

from elt.loader import count_records, ensure_tables, get_engine, load_date

email_to = os.getenv("REPORT_MAIL_TO")

# ЦБ публикует курс на дату D вечером D-1, "сегодня" у него московское
MSK = pendulum.timezone('Europe/Moscow')

def get_target_date(context) -> date:
    '''
    Дата, на которую запрашиваем курс. Одна на все таски рана.

    ds не подходит: по расписанию это вчера, при ручном запуске - сегодня.
    Берем фактическую дату старта рана: она одинакова для всех тасков
    и не меняется при ретраях.

    Перезалить конкретный день: Trigger DAG w/ config {"target_date": "2026-08-07"}
    '''
    param_date = (context['params'] or {}).get('target_date')
    if param_date:
        return date.fromisoformat(param_date)

    started = context['dag_run'].start_date or pendulum.now(MSK)
    return pendulum.instance(started).in_timezone(MSK).date()

def ensure_raw_schema():
    get_engine()
    ensure_tables()

def extract_and_load(**context):
    target_date = get_target_date(context)
    logging.info(f'Загружаем курсы на {target_date}')
    load_date(target_date)

    return target_date.isoformat() # в xcom, чтобы дата была видна в UI

def check_data_loaded(**context):
    target_date = get_target_date(context)
    if count_records(target_date) == 0:
        raise Exception(f'В БД нет записей за {target_date} после загрузки!')

def form_weekly_rep(**context):
    '''
    Эксель отчет с данными по курсам определенных валют за неделю.
    Отчет по пн при запуске по расписанию и в любой день при ручном.
    Возвращает путь к файлу, если отчет сформирован автоматом.
    '''
    target_date = get_target_date(context)
    is_manual = context['dag_run'].external_trigger

    if target_date.isoweekday() != 1 and not is_manual:
        logging.info('Не Пн, отчет не формируем')
        return

    logging.info('Формируем еженедельный отчет')
    import pandas as pd
    from sqlalchemy import text

    # прошедшая неделя: вс (день до запуска) и 6 дней до него
    week_end = target_date - timedelta(days=1)
    week_start = week_end - timedelta(days=6)

    engine = get_engine()
    df = pd.read_sql(text('''
        SELECT *, EXTRACT(WEEK FROM requested_date) AS week_num
        FROM finance_marts.dm_weekly_report
        WHERE requested_date BETWEEN :week_start AND :week_end
        ORDER BY requested_date DESC, char_code ASC
    '''),
    engine,
    params={'week_start': week_start, 'week_end': week_end})

    rep_name_end = target_date.isoformat()
    if is_manual:
        rep_name_end += '_manual'

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

    check_date = get_target_date(context).isoformat()

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text('''
            SELECT current_rate, prev_date_rate, daily_change_pct
            FROM finance_marts.dm_change_alerts
            WHERE requested_date = CAST(:dt AS DATE)
              AND alert_triggered = TRUE
        '''), {'dt': check_date}).fetchone()

        if row is None:
            logging.info('С курсом USD все нормально')
            return

        current_rate, prev_rate, change_pct = row

        logging.info('Скачок курса USD!')
        logging.info(f'Был {prev_rate}, стал {current_rate} -> изменение на {change_pct}%')

        change_dir = 'Повышение' if change_pct >= 0 else 'Снижение'

    # отправка алерта
    from airflow.providers.smtp.hooks.smtp import SmtpHook
    smtp_hook = SmtpHook(smtp_conn_id='smtp_default')

    subject = 'Скачок курса USD!'
    body = f'''
    <h1>{change_dir} курса</h1>
    <ul>
        <li><b>Предыдущее значение:</b> {prev_rate}</li>
        <li><b>Текущее значение:</b> {current_rate}</li>
        <li><b>Изменение:</b> {change_pct}%</li>
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
    params={'target_date': None}, # для перезаливки конкретного дня вручную
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
