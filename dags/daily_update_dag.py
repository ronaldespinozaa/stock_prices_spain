from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor

# Importar utilidades personalizadas
import sys
sys.path.append(os.path.dirname(os.path.abspath(file)))
from utils.stock_utils import download_stock_data, process_stock_data
from utils.db_utils import create_db_connection, create_tables, insert_stock_data

# Lista de las 10 principales acciones del IBEX 35
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
'owner': 'airflow',
'depends_on_past': False,
'email': ['tu_email@example.com'],
'email_on_failure': True,
'email_on_retry': False,
'retries': 3,
'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
'ibex35_daily_update',
default_args=default_args,
description='Actualiza diariamente los datos de las 10 principales acciones del IBEX 35',
schedule_interval='0 18 * * 1-5', # Ejecutar a las 18:00 de lunes a viernes (después del cierre del mercado)
start_date=datetime(2023, 1, 1),
tags=['ibex35', 'stocks', 'daily'],
catchup=False,
)

# Sensor para asegurar que el mercado ha cerrado
wait_for_market_close = TimeDeltaSensor(
task_id='wait_for_market_close',
delta=timedelta(minutes=15), # Esperar 15 minutos después de la hora programada
dag=dag,
)

# Tarea para verificar la base de datos
def check_database(**kwargs):
    data_path = kwargs['data_path']
    db_path = os.path.join(data_path, 'database', 'stocks.db')

    # Verificar si la base de datos existe
    if not os.path.exists(db_path):
        # Crear la base de datos y tablas
        conn = create_db_connection(db_path)
        create_tables(conn)
        conn.close()
        return db_path, False

    return db_path, True

check_db = PythonOperator(
task_id='check_database',
python_callable=check_database,
op_kwargs={'data_path': '{{ var.value.data_path }}'},
dag=dag,
)

# Tarea para descargar datos diarios de cada acción
def download_daily_data(**kwargs):
    ticker = kwargs['ticker']
    data_path = kwargs['data_path']
    execution_date = kwargs['execution_date']

    # Si es lunes, obtener datos del fin de semana también
    if execution_date.weekday() == 0:  # Lunes
        start_date = execution_date - timedelta(days=3)  # Incluir viernes, sábado y domingo
    else:
        start_date = execution_date - timedelta(days=1)

    end_date = execution_date

    # Descargar datos
    df = download_stock_data(ticker, start_date, end_date)

    # Guardar datos crudos
    raw_path = os.path.join(data_path, 'raw', f'{ticker}_daily_{execution_date.strftime("%Y%m%d")}.csv')
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
    python_callable=download_daily_data,
    op_kwargs={
    'ticker': ticker,
    'data_path': '{{ var.value.data_path }}',
    'execution_date': '{{ execution_date }}'
    },
    dag=dag,
    )
download_tasks[ticker] = download_task

# Tarea para procesar datos
def process_daily_data(ticker=ticker, **kwargs):
    ti = kwargs['ti']
    data_path = kwargs['data_path']
    execution_date = kwargs['execution_date']
    raw_path = ti.xcom_pull(task_ids=f'download_{ticker.replace(".", "_")}')
    
    # Cargar datos crudos
    df = pd.read_csv(raw_path, index_col=0, parse_dates=True)
    
    # Si no hay datos (por ejemplo, día festivo), devolver None
    if df.empty:
        return None
    
    # Procesar datos
    processed_df = process_stock_data(df, ticker)
    
    # Guardar datos procesados
    processed_path = os.path.join(data_path, 'processed', f'{ticker}_daily_{execution_date.strftime("%Y%m%d")}.csv')
    processed_df.to_csv(processed_path)
    
    return processed_path

process_task = PythonOperator(
    task_id=f'process_{ticker.replace(".", "_")}',
    python_callable=process_daily_data,
    op_kwargs={
        'data_path': '{{ var.value.data_path }}',
        'execution_date': '{{ execution_date }}'
    },
    provide_context=True,
    dag=dag,
)
process_tasks[ticker] = process_task

# Tarea para cargar datos en la base de datos
def load_to_database(ticker=ticker, **kwargs):
    ti = kwargs['ti']
    processed_path = ti.xcom_pull(task_ids=f'process_{ticker.replace(".", "_")}')
    db_path, _ = ti.xcom_pull(task_ids='check_database')
    
    # Si no hay datos procesados, salir
    if not processed_path:
        return f"No hay datos nuevos para {ticker}"
    
    # Cargar datos procesados
    df = pd.read_csv(processed_path, index_col=0, parse_dates=True)
    
    # Insertar en la base de datos
    conn = create_db_connection(db_path)
    insert_stock_data(conn, df, ticker, 'daily')
    conn.close()
    
    return f"Datos diarios de {ticker} cargados en la base de datos"

load_task = PythonOperator(
    task_id=f'load_{ticker.replace(".", "_")}',
    python_callable=load_to_database,
    provide_context=True,
    dag=dag,
)
load_tasks[ticker] = load_task
# Tarea para actualizar el resumen
def update_summary(**kwargs):
    ti = kwargs['ti']
    data_path = kwargs['data_path']
    db_path, _ = ti.xcom_pull(task_ids='check_database')
    execution_date = kwargs['execution_date']

    # Conectar a la base de datos
    conn = create_db_connection(db_path)

    # Consultar datos para el resumen
    import sqlite3
    cursor = conn.cursor()

    # Resumen de datos diarios
    cursor.execute('''
        SELECT ticker, COUNT(*) as records, MAX(date) as last_update
        FROM daily_data
        GROUP BY ticker
    ''')
    daily_summary = pd.DataFrame(cursor.fetchall(), columns=['ticker', 'daily_records', 'last_update'])

    # Resumen de datos históricos
    cursor.execute('''
        SELECT ticker, COUNT(*) as records, MIN(date) as start_date, MAX(date) as end_date
        FROM historical_data
        GROUP BY ticker
    ''')
    historical_summary = pd.DataFrame(cursor.fetchall(), columns=['ticker', 'historical_records', 'start_date', 'end_date'])

    # Combinar resúmenes
    if not historical_summary.empty:
        summary = pd.merge(daily_summary, historical_summary, on='ticker', how='outer')
    else:
        summary = daily_summary
        summary['historical_records'] = 0
        summary['start_date'] = None
        summary['end_date'] = None

    # Guardar resumen
    summary_path = os.path.join(data_path, 'processed', f'summary_{execution_date.strftime("%Y%m%d")}.csv')
    summary.to_csv(summary_path, index=False)

    conn.close()

    return summary_path

update_summary_task = PythonOperator(
task_id='update_summary',
python_callable=update_summary,
op_kwargs={
'data_path': '{{ var.value.data_path }}',
'execution_date': '{{ execution_date }}'
},
provide_context=True,
dag=dag,
)

# Definir dependencias
wait_for_market_close >> check_db

for ticker in IBEX35_TOP10:
    check_db >> download_tasks[ticker] >> process_tasks[ticker] >> load_tasks[ticker] >> update_summary_task