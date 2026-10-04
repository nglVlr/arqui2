# Conexión a PostgreSQL compartida por los 3 DAGs de esta carpeta.
# Usa el usuario de la aplicación (variables de entorno del contenedor de Airflow).
# Airflow ignora este archivo como DAG porque no define ninguno.

import os

import psycopg2


def conectar():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("APP_DB_USER"),
        password=os.getenv("APP_DB_PASSWORD"),
    )
