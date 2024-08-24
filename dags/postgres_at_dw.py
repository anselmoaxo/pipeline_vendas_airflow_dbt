from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash_operator import BashOperator

# Definindo os argumentos padrão para a DAG
# Esses parâmetros definem configurações básicas, como quem é o dono da DAG, 
# se a DAG depende de execuções passadas, a data de início, entre outros.
default_args = {
    'owner': 'airflow',  # Nome do proprietário da DAG
    'depends_on_past': False,  # A DAG não depende de execuções passadas
    'start_date': datetime(2024, 1, 1),  # Data de início da DAG
    'email_on_failure': False,  # Desativa notificação por e-mail em caso de falha
    'email_on_retry': False,  # Desativa notificação por e-mail em caso de retry
    'retries': 0,  # Número de tentativas de retry em caso de falha
    'retry_delay': timedelta(minutes=1),  # Intervalo entre tentativas de retry
}

# Diretório do projeto dbt
#DBT_PROJECT_DIR = "/home/anselmo/curso-airflow/dbt_project"

# Definição da DAG usando o decorator `@dag`
@dag(
    dag_id='postgres_to_postgres',  # Identificador único da DAG
    default_args=default_args,  # Argumentos padrão definidos acima
    description='Load data incrementally from one Postgres to another',  # Descrição da DAG
    schedule_interval=timedelta(days=1),  # Frequência de execução (diária)
    catchup=False  # Evita execuções retroativas para datas anteriores
)
def postgres_to_postgres_etl():
    # Lista das tabelas a serem processadas
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']
    schema_destino = 'stage'  # Esquema de destino no banco de dados de destino
    
    # Tarefa para obter a chave primária máxima de uma tabela no banco de dados de destino
    @task
    def get_max_primary_key(table_name: str):
        # Conecta ao banco de dados de destino usando a conexão configurada no Airflow
        with PostgresHook(postgres_conn_id='postgres_destino').get_conn() as conn:
            with conn.cursor() as cursor:
                # Seleciona o maior valor da chave primária na tabela de destino
                cursor.execute(f"SELECT MAX(id_{table_name}) FROM {schema_destino}.{table_name}")
                max_id = cursor.fetchone()[0]
                # Retorna 0 se a tabela estiver vazia, caso contrário retorna o maior ID
                return max_id if max_id is not None else 0
    
    # Tarefa para carregar dados incrementais de uma tabela do banco de dados de origem para o destino
    @task
    def load_incremental_data(table_name: str, max_id: int):
        # Conecta ao banco de dados de origem
        with PostgresHook(postgres_conn_id='postgres_novadrive').get_conn() as pg_conn:
            with pg_conn.cursor() as pg_cursor:
                primary_key = f'id_{table_name}'  # Define a chave primária com base no nome da tabela
                
                # Obtém a lista de colunas da tabela de origem
                pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}'")
                columns = [row[0] for row in pg_cursor.fetchall()]
                columns_list_str = ', '.join(columns)  # Concatena as colunas em uma string
                placeholders = ', '.join(['%s'] * len(columns))  # Cria placeholders para valores na query de inserção
                
                # Seleciona os registros que possuem a chave primária maior que o maior ID no destino
                pg_cursor.execute(f"SELECT {columns_list_str} FROM {table_name} WHERE {primary_key} > %s", (max_id,))
                rows = pg_cursor.fetchall()
                
                # Conecta ao banco de dados de destino para inserir os dados incrementais
                with PostgresHook(postgres_conn_id='postgres_destino').get_conn() as dest_conn:
                    with dest_conn.cursor() as dest_cursor:
                        # Query de inserção no banco de dados de destino
                        insert_query = f"INSERT INTO {schema_destino}.{table_name} ({columns_list_str}) VALUES ({placeholders})"
                        # Insere cada linha de dados
                        for row in rows:
                            dest_cursor.execute(insert_query, row)
    
    # Definição das tarefas de obtenção do maior ID por tabela
    max_id_tasks = {table_name: get_max_primary_key(table_name) for table_name in table_names}
    # Definição das tarefas de carregamento incremental por tabela
    load_data_tasks = {table_name: load_incremental_data(table_name, max_id_tasks[table_name]) for table_name in table_names}
    
    # Configura a ordem de execução das tarefas
    for table_name in table_names:
        max_id_tasks[table_name] >> load_data_tasks[table_name]

    # Tarefa para executar o dbt após todas as tarefas de carga de dados
   # dbt_run = BashOperator(
       # task_id="dbt_run",  # Identificador único da tarefa no Airflow
        #bash_command=f"source /path/to/venv/bin/activate && dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"

    # Configura a execução do dbt_run após todas as tarefas de carga de dados
    for table_name in table_names:
        load_data_tasks[table_name] 

# Instancia a DAG para ser reconhecida pelo Airflow
postgres_to_postgres_etl_dag = postgres_to_postgres_etl()
