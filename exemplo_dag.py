from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Argumentos padrão para a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
dag = DAG(
    'exemplo_hello_world',
    default_args=default_args,
    description='Uma DAG de exemplo simples para testar o Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['exemplo', 'teste'],
)

# Tarefa que executa um comando bash
t1 = BashOperator(
    task_id='print_date',
    bash_command='date > /tmp/current_date.txt && echo "Data atual salva em /tmp/current_date.txt"',
    dag=dag,
)

# Função Python para ser executada por uma tarefa
def print_hello():
    print("Olá do Airflow!")
    return 'Olá do Airflow!'

# Tarefa que executa uma função Python
t2 = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Definição da ordem de execução das tarefas
t1 >> t2 