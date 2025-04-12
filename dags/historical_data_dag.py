from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


# Lista de las 10 principales acciones del IBEX 35
import sys
sys.path.append(os.path.dirname(os.path.abspath(file)))
from utils.stock_utils import download_stock_data, process_stock_data
from utils.db_utils import create_db_connection, create_tables, insert_stock_data

#Definir argumentos por defecto para el DAG
IBEX35_TOP10 = [
'SAN.MC', # Banco Santander
'BBVA.MC', # BBVA
'IBE.MC', # Iberdrola
'ITX.MC', # Inditex
'TEF.MC', # Telefónica
'REP.MC', # Repsol
'CABK.MC', # CaixaBank
'NTGY.MC', # Naturgy
'FER.MC', # Ferrovial
'AMS.MC' # Amadeus
]

# Definir argumentos por defecto para el DAG
default_args = {
'owner': 'airflow', # Nombre del propietario del DAG
'depends_on_past': False, # No depende de ejecuciones pasadas
'email': ['tu_email@example.com'], #
'email_on_failure': False, # No enviar email en caso de fallo
'email_on_retry': False, # No enviar email en caso de reintento
'retries': 1, # Número de reintentos en caso de fallo
'retry_delay': timedelta(minutes=5), # Esperar 5 minutos entre reintentos
}

# Definir el DAG

dag = DAG(
'ibex35_historical_data',
default_args=default_args,
description='Extrae datos históricos de las 10 principales acciones del IBEX 35',
schedule_interval=None, # Este DAG se ejecuta manualmente
start_date=days_ago(1),
tags=['ibex35', 'stocks', 'historical'],
catchup=False,
)

# Tarea para crear directorios necesarios

create_directories = BashOperator(
task_id='create_directories',
bash_command='mkdir -p {{ var.value.data_path }}/raw {{ var.value.data_path }}/processed {{ var.value.data_path }}/database',
dag=dag,
)

# Tarea para crear la base de datos y tablas

def setup_database(**kwargs):
    db_path = os.path.join(kwargs['data_path'], 'database', 'stocks.db')
    conn = create_db_connection(db_path)
    create_tables(conn)
    conn.close()
    return db_path

setup_db = PythonOperator(
task_id='setup_database',
python_callable=setup_database,
op_kwargs={'data_path': '{{ var.value.data_path }}'},
dag=dag,
)

# Tarea para descargar datos históricos de cada acción

def download_historical_data(**kwargs):
    ticker = kwargs['ticker']
    data_path = kwargs['data_path']
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365*2) # 2 años de datos

    # Descargar datos
    df = download_stock_data(ticker, start_date, end_date)

    # Guardar datos crudos
    raw_path = os.path.join(data_path, 'raw', f'{ticker}_historical.csv')
    df.to_csv(raw_path)

    return raw_path

# Crear tareas dinámicamente para cada acción

download_tasks = {}
process_tasks = {}
load_tasks = {}

for ticker in IBEX35_TOP10:
    # Tarea para descargar datos
    download_task = PythonOperator(
    task_id=f'download_{ticker.replace(".", "_")}',
    python_callable=download_historical_data,
    op_kwargs={'ticker': ticker, 'data_path': '{{ var.value.data_path }}'},
    dag=dag,
    )
    download_tasks[ticker] = download_task

# Tarea para procesar datos
def process_historical_data(ticker=ticker, **kwargs):
    ti = kwargs['ti']
    data_path = kwargs['data_path']
    raw_path = ti.xcom_pull(task_ids=f'download_{ticker.replace(".", "_")}')
    
    # Cargar datos crudos
    df = pd.read_csv(raw_path, index_col=0, parse_dates=True)
    
    # Procesar datos
    processed_df = process_stock_data(df, ticker)
    
    # Guardar datos procesados
    processed_path = os.path.join(data_path, 'processed', f'{ticker}_processed.csv')
    processed_df.to_csv(processed_path)
    
    return processed_path

process_task = PythonOperator(
    task_id=f'process_{ticker.replace(".", "_")}',
    python_callable=process_historical_data,
    op_kwargs={'data_path': '{{ var.value.data_path }}'},
    provide_context=True,
    dag=dag,
)
process_tasks[ticker] = process_task

# Tarea para cargar datos en la base de datos
def load_to_database(ticker=ticker, **kwargs):
    ti = kwargs['ti']
    data_path = kwargs['data_path']
    processed_path = ti.xcom_pull(task_ids=f'process_{ticker.replace(".", "_")}')
    db_path = ti.xcom_pull(task_ids='setup_database')
    
    # Cargar datos procesados
    df = pd.read_csv(processed_path, index_col=0, parse_dates=True)
    
    # Insertar en la base de datos
    conn = create_db_connection(db_path)
    insert_stock_data(conn, df, ticker, 'historical')
    conn.close()
    
    return f"Datos de {ticker} cargados en la base de datos"

load_task = PythonOperator(
    task_id=f'load_{ticker.replace(".", "_")}',
    python_callable=load_to_database,
    op_kwargs={'data_path': '{{ var.value.data_path }}'},
    provide_context=True,
    dag=dag,
)
load_tasks[ticker] = load_task

#Tarea final para generar un resumen
def generate_summary(**kwargs):
    ti = kwargs['ti']
    data_path = kwargs['data_path']
    db_path = ti.xcom_pull(task_ids='setup_database')

    # Conectar a la base de datos
    conn = create_db_connection(db_path)

    # Consultar datos para el resumen
    import sqlite3
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ticker, COUNT(*) as records, MIN(date) as start_date, MAX(date) as end_date
        FROM historical_data
        GROUP BY ticker
    ''')

    # Crear DataFrame con el resumen
    summary = pd.DataFrame(cursor.fetchall(), columns=['ticker', 'records', 'start_date', 'end_date'])

    # Guardar resumen
    summary_path = os.path.join(data_path, 'processed', 'historical_summary.csv')
    summary.to_csv(summary_path, index=False)

    conn.close()

    return summary_path

# Tarea final para generar un resumen

generate_summary_task = PythonOperator(
task_id='generate_summary',
python_callable=generate_summary,
op_kwargs={'data_path': '{{ var.value.data_path }}'},
provide_context=True,
dag=dag,
)

# Definir dependencias
create_directories >> setup_db

for ticker in IBEX35_TOP10:
    setup_db >> download_tasks[ticker] >> process_tasks[ticker] >> load_tasks[ticker] >> generate_summary_task