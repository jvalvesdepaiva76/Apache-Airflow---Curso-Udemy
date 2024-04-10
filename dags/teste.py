from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

#função que será executada pela tarefa
def hello():
    print("Hello world")

#data e hora atual
now = datetime.now()

#Criando uma instância da DAG com o nome 'teste', data de início definida como a data atual,
# e intervalo de agendamento de execução a cada 30 minutos
with DAG('teste', start_date=now,
         schedule_interval='30 * * * *', catchup=False) as dag:
  
    # Definindo a tarefa PythonOperator chamada 'Hello_World' que executará a função 'hello'
    helloWorld = PythonOperator(
        task_id='Hello_World',
        python_callable=hello
    )

  #Em DAGs mais complexas com múltiplas tarefas, é importante definir a ordem de execução das tarefas para garantir que elas sejam executadas na sequência desejada. No entanto, como a DAG atualmente possui apenas uma tarefa, isso não é necessário.