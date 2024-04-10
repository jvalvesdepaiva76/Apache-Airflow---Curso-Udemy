from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def get_url():
    return 'https://api.adviceslip.com/advice'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('advice_slip',
        default_args=default_args,
        schedule_interval='30 6 * * *',
        catchup=False) as dag:
  
    get_advice_task = SimpleHttpOperator(
    task_id='get_advice',
    http_conn_id='advice_api',
    method='GET',
    endpoint='', #string vazia por que está configurado na conexão
    headers={}, #headers vazio indica que nenhum cabeçalho adicional é fornecido para incluir na requisição http
    response_check=lambda response: True if response.json().get('slip') else False, #função que verifica se o JSON de resposta contém a chave "slip"
    xcom_push=True,
    dag=dag
)
