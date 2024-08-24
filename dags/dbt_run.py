from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Defina o DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_run_dag',
    default_args=default_args,
    description='Um DAG para executar dbt run',
    schedule_interval='@daily',  # ajuste conforme necessário
    start_date=days_ago(1),
)

# Defina a tarefa para executar dbt run
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /home/anselmo/curso-airflow/dbt_project && dbt run',  # ajuste o caminho
    dag=dag,
)

dbt_run
