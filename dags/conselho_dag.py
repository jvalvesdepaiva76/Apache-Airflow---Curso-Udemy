#datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator

# initializing the default arguments
default_args = {
		'owner': 'Victor',
		'start_date': datetime(2024, 4, 14),
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
advice_dag = DAG('advice_dag',
		default_args=default_args,
		description='Conselho DAG',
		schedule_interval='* * * * *', 
		catchup=False,
		tags=['exercicio2, conselho']
)

# python callable function
def get_url():
		return 'https://api.adviceslip.com/advice'

# Creating first task
start_task = DummyOperator(task_id='start_task', dag=advice_dag)

# Creating second task
get_advice_task = SimpleHttpOperator(
    task_id='get_advice',
    http_conn_id='advice_api',
    method='GET',
    endpoint='', #string vazia por que está configurado na conexão
    headers={}, #headers vazio indica que nenhum cabeçalho adicional é fornecido para incluir na requisição http
    response_check=lambda response: True if response.json().get('slip') else False, #função que verifica se o JSON de resposta contém a chave "slip"
	dag=advice_dag
)


# Creating third task
end_task = DummyOperator(task_id='end_task', dag=advice_dag)

# Set the order of execution of tasks. 
start_task >> get_advice_task >> end_task