from datetime import timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sys

# Rutas
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.stock_utils import download_stock_data, process_stock_data
from utils.db_utils import create_db_connection, create_database, save_to_database

data_path = os.environ.get('DATA_PATH', '/opt/airflow/data')

IBEX35_TOP10 = [
    'SAN.MC'#, 'BBVA.MC', 'IBE.MC', 'ITX.MC', 'TEF.MC',
    # 'REP.MC', 'CABK.MC', 'NTGY.MC', 'FER.MC', 'AMS.MC'
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['tu_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

dag = DAG(
    'ibex35_historical_data',
    default_args=default_args,
    description='Extrae datos históricos de las 10 principales acciones del IBEX 35',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['ibex35', 'stocks', 'historical']
)

create_directories = BashOperator(
    task_id='create_directories',
    bash_command=f'mkdir -p {data_path}/raw {data_path}/processed {data_path}/database',
    dag=dag,
)

def setup_database(**kwargs):
    db_path = os.path.join(kwargs['data_path'], 'database', 'stocks.db')
    conn = create_db_connection(db_path)
    create_database(conn)
    conn.close()
    return db_path

setup_db = PythonOperator(
    task_id='setup_database',
    python_callable=setup_database,
    op_kwargs={'data_path': data_path},
    dag=dag,
)

download_tasks = {}
process_tasks = {}
load_tasks = {}

for ticker in IBEX35_TOP10:
    def download_historical_data(ticker=ticker, **kwargs):
        data_path = kwargs['data_path']
        raw_path = os.path.join(data_path, 'raw', f'{ticker}_historical.csv')

        # # No volver a descargar si ya existe
        # if os.path.exists(raw_path):
        #     print(f"Archivo {raw_path} ya existe. Omitiendo descarga.")
        #     return raw_path

        df = download_stock_data(ticker, period='1y')
        df.to_csv(raw_path)
        return raw_path

    download_task = PythonOperator(
        task_id=f'download_{ticker.replace(".", "_")}',
        python_callable=download_historical_data,
        op_kwargs={'data_path': data_path},
        dag=dag,
    )
    download_tasks[ticker] = download_task

    def process_historical_data(ticker=ticker, **kwargs):
        ti = kwargs['ti']
        data_path = kwargs['data_path']
        raw_path = ti.xcom_pull(task_ids=f'download_{ticker.replace(".", "_")}')
        processed_path = os.path.join(data_path, 'processed', f'{ticker}_processed.csv')

        # # No reprocesar si ya existe
        # if os.path.exists(processed_path):
        #     print(f"Archivo {processed_path} ya existe. Omitiendo procesamiento.")
        #     return processed_path

        df = pd.read_csv(raw_path, index_col=0, parse_dates=True)
        processed_df = process_stock_data(df, ticker)
        processed_df.to_csv(processed_path)
        return processed_path

    process_task = PythonOperator(
        task_id=f'process_{ticker.replace(".", "_")}',
        python_callable=process_historical_data,
        op_kwargs={'data_path': data_path},
        provide_context=True,
        dag=dag,
    )
    process_tasks[ticker] = process_task

    def load_to_database(ticker=ticker, **kwargs):
        ti = kwargs['ti']
        data_path = kwargs['data_path']
        processed_path = ti.xcom_pull(task_ids=f'process_{ticker.replace(".", "_")}')
        db_path = ti.xcom_pull(task_ids='setup_database')

        conn = create_db_connection(db_path)
        cursor = conn.cursor()

        df = pd.read_csv(processed_path, index_col=0, parse_dates=True)
        if not df.empty:
            cursor.execute("SELECT MAX(date) FROM historical_data WHERE ticker = ?", (ticker,))
            last_date = cursor.fetchone()[0]

            if last_date:
                df = df[df.index > pd.to_datetime(last_date)]

        if not df.empty:
            save_to_database(conn, df, ticker, 'historical')
            print(f"{len(df)} filas insertadas en la base de datos para {ticker}.")
        else:
            print(f"No hay datos nuevos para {ticker}. No se insertó nada.")

        conn.close()
        return f"Datos de {ticker} cargados en la base de datos"

    load_task = PythonOperator(
        task_id=f'load_{ticker.replace(".", "_")}',
        python_callable=load_to_database,
        op_kwargs={'data_path': data_path},
        provide_context=True,
        dag=dag,
    )
    load_tasks[ticker] = load_task

def generate_summary(**kwargs):
    ti = kwargs['ti']
    data_path = kwargs['data_path']
    db_path = ti.xcom_pull(task_ids='setup_database')

    conn = create_db_connection(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT ticker, COUNT(*) as records, MIN(date) as start_date, MAX(date) as end_date
        FROM historical_data
        GROUP BY ticker
    ''')

    summary = pd.DataFrame(cursor.fetchall(), columns=['ticker', 'records', 'start_date', 'end_date'])
    summary_path = os.path.join(data_path, 'processed', 'historical_summary.csv')
    summary.to_csv(summary_path, index=False)
    conn.close()
    return summary_path

generate_summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    op_kwargs={'data_path': data_path},
    provide_context=True,
    dag=dag,
)

# Dependencias
create_directories >> setup_db

for ticker in IBEX35_TOP10:
    setup_db >> download_tasks[ticker] >> process_tasks[ticker] >> load_tasks[ticker] >> generate_summary_task
