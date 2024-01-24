from airflow import DAG
from airflow.operators.bash import  BashOperator 
from airflow.operators.python  import PythonOperator
from pendulum import DateTime
from datetime import datetime

dag = DAG('xcom', description= "Utilizando xcom", schedule_interval=None,
          start_date=datetime(2024,1,15),catchup=False)


def task_write(**kwargs):
    kwargs['ti'].xcom_push(key='valorxcom1',value=10)
task1 = PythonOperator(task_id="tsk1", python_callable=task_write, dag=dag)

def task_read(**kwargs):
    valor = kwargs['ti'].xcom_pull(key='valorxcom1')
    print(f"valor recuperado : {valor}")

task2 = PythonOperator(task_id="tsk2", python_callable=task_read, dag=dag)

# Sequencia de execução das tasks

task1 >> task2 