from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from os.path import join
from dotenv import load_dotenv
import os


dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Acessar variáveis
token = os.getenv('TOKEN')

def extrai_dados(data_inicio, data_fim):
    print(f"Token: {token}")  # Para desenvolvimento, use print

    city ='Guarulhos'
    key=token

    URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
        f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')
    dados = pd.read_csv(URL)

    # Conectar ao banco de dados PostgreSQL usando PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres')  # Substitua pelo ID da sua conexão
    engine = hook.get_sqlalchemy_engine()

    # Inserir dados no PostgreSQL
    dados.to_sql('dados_climaticos', engine, if_exists='replace', index=False)
    dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_sql('temperaturas', engine, if_exists='replace', index=False)
    dados[['datetime', 'description', 'icon']].to_sql('condicoes', engine, if_exists='replace', index=False)

with DAG(
        "dados_climaticos",
        start_date=pendulum.datetime(2024, 7, 1, tz="UTC"),
        schedule_interval='0 0 * * 1',  # executar toda segunda-feira
        catchup=False
) as dag:

    tarefa_2 = PythonOperator(
        task_id='extrai_dados',
        python_callable=extrai_dados,
        op_kwargs={
            'data_inicio': '{{ ds }}',
            'data_fim': '{{ macros.ds_add(ds, 15) }}'  # Usar macros.ds_add para adicionar dias
        }
    )

    tarefa_2