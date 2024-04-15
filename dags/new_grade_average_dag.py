import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor

def calculate_average_grades():
    input_file_path = '/opt/airflow/dags/_resources/input.csv'
    output_file_path = '/opt/airflow/dags/_resources/output.csv'

    averages = []

    with open(input_file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            grades = [float(grade) for grade in row]
            average = sum(grades) / len(grades)
            averages.append(average)

    print(averages)

    with open(output_file_path, 'w') as file:
        writer = csv.writer(file)
        for average in averages:
            writer.writerow([average])

default_args = {
    'owner': 'Victor',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

grade_average_dag = DAG('grade_average_dag',
          default_args=default_args,
          description='DAG to calculate the average grades of students',
          schedule_interval=None,
          catchup=False,
          tags=['exercicio3, grades_average'])

start_task = DummyOperator(task_id='start_task', dag=grade_average_dag)

file_sensor_task = FileSensor(
    task_id='check_file_existence',
    filepath='/opt/airflow/dags/_resources/input.csv',
    poke_interval=5,
    timeout=5 * 60,
    mode='poke',
    dag=grade_average_dag
)

calculate_average_task = PythonOperator(
    task_id='average_grades_students',
    python_callable=calculate_average_grades,
    dag=grade_average_dag
)

end_task = DummyOperator(task_id='end_task', dag=grade_average_dag)

# Definir a ordem de execução das tarefas
start_task >> file_sensor_task >> calculate_average_task >> end_task
