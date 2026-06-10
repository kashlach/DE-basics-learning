from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import random

# 1. Функция, которую будем вызывать из PythonOperator
def print_random_number():
    number = random.randint(1, 100)
    print(f"Случайное число: {number}")
    return number

# 2. Ещё одна функция
def check_even_or_odd(**context):
    # Забираем результат предыдущей задачи (XCom)
    number = context['ti'].xcom_pull(task_ids='generate_random')
    
    if number % 2 == 0:
        print(f"Число {number} — чётное")
        result = "even"
    else:
        print(f"Число {number} — нечётное")
        result = "odd"
    
    return result

# 3. Аргументы по умолчанию для всех задач в DAG
default_args = {
    'owner': 'pypypy',                  # кто владелец DAG
    'depends_on_past': False,           # не зависит от прошлых запусков
    'start_date': datetime(2025, 6, 1), # с какой даты можно запускать
    'email_on_failure': False,          # не слать письма при ошибке
    'email_on_retry': False,            # не слать письма при повторе
    'retries': 1,                       # сколько раз повторить при ошибке
    'retry_delay': timedelta(minutes=1) # ждать 1 минуту перед повтором
}

# 4. Создаём DAG
dag = DAG(
    'AI_generated_dag',                 # уникальное имя DAG
    default_args=default_args,
    description='Генерация и анализ чисел',
    schedule_interval='@hourly',        # запускать каждый час
    catchup=False,                      # не запускать пропущенные интервалы
    tags=['getting_to_know', 'random']  # теги для фильтрации в UI
)

# 5. Задача 1: через BashOperator (просто выполнить команду)
# Логирование начала пайплайна
task_hello = BashOperator(
    task_id='say_hello',
    bash_command='echo "Airflow запустил DAG в $(date)"',
    dag=dag
)

# 6. Задача 2: через PythonOperator (вызвать функцию)
# Имитация получения данных
task_generate = PythonOperator(
    task_id='generate_random',
    python_callable=print_random_number,
    dag=dag
)

# 7. Задача 3: ещё один PythonOperator (с передачей контекста)
# Передача данных между задачами через XCom
task_analyze = PythonOperator(
    task_id='analyze_even_odd',
    python_callable=check_even_or_odd,
    provide_context=True,               # нужно, чтобы получать результат из xcom
    dag=dag
)

# 8. Задача 4: просто информационный BashOperator
task_done = BashOperator(
    task_id='say_goodbye',
    bash_command='echo "DAG завершён. Анализ выполнен!"',
    dag=dag
)

# 9. Определяем порядок выполнения (граф зависимостей)
task_hello >> task_generate >> task_analyze >> task_done
