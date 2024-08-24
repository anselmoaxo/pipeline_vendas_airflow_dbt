from sqlalchemy import create_engine

# Substitua os valores com suas credenciais e endereço
engine = create_engine('postgresql+psycopg2://postgres:postgres@172.17.64.1:5432/destino_dw')

try:
    with engine.connect() as connection:
        print("Conexão bem-sucedida!")
except Exception as e:
    print(f"Erro ao conectar: {e}")