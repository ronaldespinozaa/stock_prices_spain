from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Importar utilidades personalizadas
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.stock_utils import download_daily_data, process_stock_data
from utils.db_utils import create_db_connection, create_database, save_to_database

data_path = os.environ.get('DATA_PATH', '/opt/airflow/data')

# Lista de las 10 principales acciones del IBEX 35
IBEX35_TOP10 = [
    'SAN.MC', 'BBVA.MC', 'IBE.MC', 'ITX.MC', 'TEF.MC', 'REP.MC',
    'CABK.MC', 'NTGY.MC', 'FER.MC', 'AMS.MC'
]

# Definir argumentos por defecto para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['tu_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'ibex35_daily_update',
    default_args=default_args,
    description='Actualiza diariamente los datos de las 10 principales acciones del IBEX 35',
    schedule_interval='45 17 * * 1-5',  # Ejecutar a las 17:45 hora Madrid
    start_date=datetime(2023, 1, 1),
    tags=['ibex35', 'stocks', 'daily'],
    catchup=False,
)

# Tarea: verificar la base de datos
def check_database(**kwargs):
    db_path = os.path.join(data_path, 'database', 'stocks.db')
    if not os.path.exists(db_path):
        conn = create_db_connection(db_path)
        create_database(conn)
        conn.close()
    return db_path

check_db = PythonOperator(
    task_id='check_database',
    python_callable=check_database,
    dag=dag,
)

# Generar tareas dinámicas
download_tasks = {}
process_tasks = {}
load_tasks = {}

for ticker in IBEX35_TOP10:
    # Función para descargar datos
    def create_download_function(ticker):
        def _download_daily_data(**kwargs):
            ds = kwargs['ds']
            df = download_daily_data([ticker], '1d', ds, ds)  # Descarga para el ticker especificado
            raw_path = os.path.join(data_path, 'raw', f'{ticker}_daily_{ds}.csv')
            df[ticker].to_csv(raw_path)
            return raw_path
        return _download_daily_data

    download_tasks[ticker] = PythonOperator(
        task_id=f'download_{ticker.replace(".", "_")}',
        python_callable=create_download_function(ticker),
        dag=dag,
    )

    # Función para procesar datos
    def create_process_function(ticker):
        def process_daily_data(**kwargs):
            ti = kwargs['ti']
            ds = kwargs['ds']
            raw_path = ti.xcom_pull(task_ids=f'download_{ticker.replace(".", "_")}')
            df = pd.read_csv(raw_path, index_col=0, parse_dates=True)
            if df.empty:
                return None
            data_dict = {ticker: df}
            processed_df = process_stock_data(data_dict)
            processed_path = os.path.join(data_path, 'processed', f'{ticker}_daily_{ds}.csv')
            processed_df.to_csv(processed_path)
            return processed_path
        return process_daily_data

    process_tasks[ticker] = PythonOperator(
        task_id=f'process_{ticker.replace(".", "_")}',
        python_callable=create_process_function(ticker),
        dag=dag,
    )

    # Función para cargar datos a la base de datos
    def create_load_function(ticker):
        def load_to_database(**kwargs):
            ti = kwargs['ti']
            processed_path = ti.xcom_pull(task_ids=f'process_{ticker.replace(".", "_")}')
            db_path = ti.xcom_pull(task_ids='check_database')
            if not processed_path:
                return f"No hay datos nuevos para {ticker}"
            df = pd.read_csv(processed_path, index_col=0, parse_dates=True)
            conn = create_db_connection(db_path)
            save_to_database(conn, df, ticker, 'daily')
            conn.close()
            return f"Datos diarios de {ticker} cargados"
        return load_to_database

    load_tasks[ticker] = PythonOperator(
        task_id=f'load_{ticker.replace(".", "_")}',
        python_callable=create_load_function(ticker),
        dag=dag,
    )

# Tarea final: actualizar resumen
def update_summary(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds']
    db_path = ti.xcom_pull(task_ids='check_database')
    conn = create_db_connection(db_path)
    cursor = conn.cursor()

    # Resumen diario
    cursor.execute('''
        SELECT ticker, COUNT(*) as records, MAX(date) as last_update
        FROM daily_data
        GROUP BY ticker
    ''')
    daily_summary = pd.DataFrame(cursor.fetchall(), columns=['ticker', 'daily_records', 'last_update'])

    # Resumen histórico
    cursor.execute('''
        SELECT ticker, COUNT(*) as records, MIN(date) as start_date, MAX(date) as end_date
        FROM historical_data
        GROUP BY ticker
    ''')
    historical_summary = pd.DataFrame(cursor.fetchall(), columns=['ticker', 'historical_records', 'start_date', 'end_date'])

    if not historical_summary.empty:
        summary = pd.merge(daily_summary, historical_summary, on='ticker', how='outer')
    else:
        summary = daily_summary
        summary['historical_records'] = 0
        summary['start_date'] = None
        summary['end_date'] = None

    summary_path = os.path.join(data_path, 'processed', f'summary_{ds}.csv')
    summary.to_csv(summary_path, index=False)
    conn.close()
    return summary_path

update_summary_task = PythonOperator(
    task_id='update_summary',
    python_callable=update_summary,
    dag=dag,
)

# Establecer dependencias
check_db

for ticker in IBEX35_TOP10:
    check_db >> download_tasks[ticker] >> process_tasks[ticker] >> load_tasks[ticker] >> update_summary_task
