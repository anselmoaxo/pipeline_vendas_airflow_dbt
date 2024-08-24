# Pipeline de Dados com Airflow e dbt usando Astro Dev

Este projeto configura um pipeline de dados que realiza a integração entre um banco de dados de origem e um Data Warehouse (DW) PostgreSQL, utilizando Airflow para orquestração e dbt para transformação dos dados, com o auxílio do Astro Dev para desenvolvimento local.

## Estrutura do Projeto

1. **Airflow**: Orquestra o pipeline de ETL (Extração, Transformação e Carga).
2. **dbt**: Realiza transformações dos dados no Data Warehouse.
3. **Astro Dev**: Facilita o desenvolvimento e execução local do Airflow.

## Requisitos

- Docker
- Docker Compose
- Astro CLI
- PostgreSQL
- Python

## Configuração do Ambiente

### 1. Configuração do Docker

Certifique-se de ter o Docker e Docker Compose instalados. O Astro CLI cuidará da configuração do Airflow e do PostgreSQL.

### 2. Instalação do Astro CLI

Para instalar o Astro CLI, execute:

```bash
pip install astro-cli
