from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Obter o logger do Airflow
logger = logging.getLogger("airflow.task")

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
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['exemplo', 'teste'],
)

# Função Python para ser executada por uma tarefa
def log_hello():
    logger.info("Olá do Airflow!")
    logger.info("Esta mensagem será exibida nos logs do Airflow")
    logger.info("Você pode ver essa mensagem nos logs da tarefa")
    return 'Olá do Airflow!'

# Tarefa que executa uma função Python
t2 = PythonOperator(
    task_id='log_hello',
    python_callable=log_hello,
    dag=dag,
)