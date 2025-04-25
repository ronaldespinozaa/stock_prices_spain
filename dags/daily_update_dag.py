from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor

# Importar utilidades personalizadas
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.stock_utils import download_stock_data, process_stock_data
from utils.db_utils import create_db_connection,create_database,save_to_database

data_path = os.environ.get('DATA_PATH', '/opt/airflow/data')

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
    # Obtener la ruta de datos desde la variable de entorno
    data_path = os.environ.get('DATA_PATH', '/opt/airflow/data')
    db_path = os.path.join(data_path, 'database', 'stocks.db')

    # Verificar si la base de datos existe
    if not os.path.exists(db_path):
        # Crear la base de datos y tablas
        conn = create_db_connection(db_path)
        create_database(conn)
        conn.close()
        return db_path, False

    return db_path, True

check_db = PythonOperator(
    task_id='check_database',
    python_callable=check_database,
    # No más referencia a var.value.data_path
    dag=dag,
)

# Tarea para descargar datos diarios de cada acción
def download_daily_data(**kwargs):
    ticker = kwargs['ticker']
    data_path = kwargs['data_path']
    execution_date = kwargs['execution_date']
    period = kwargs.get('period', '1d')  # Usa '1d' por defecto si no se pasa un periodo

    # Convertir execution_date a datetime si es un string
    if isinstance(execution_date, str):
        execution_date = datetime.fromisoformat(execution_date.replace('Z', '+00:00'))
    
    # Descargar datos con 'period'
    df = download_stock_data(ticker, period)  # Usamos el parámetro 'period' aquí

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
            'data_path': data_path,
            'execution_date': '{{ execution_date }}'
            },
    dag=dag,
    )
    download_tasks[ticker] = download_task

# Tarea para procesar datos
    def create_process_function(specific_ticker):
            def process_daily_data(**kwargs):
                ti = kwargs['ti']
                data_path = kwargs['data_path']
                execution_date = kwargs['execution_date']
                raw_path = ti.xcom_pull(task_ids=f'download_{specific_ticker.replace(".", "_")}')
                
                # Cargar datos crudos
                df = pd.read_csv(raw_path, index_col=0, parse_dates=True)
                
                # Si no hay datos (por ejemplo, día festivo), devolver None
                if df.empty:
                    return None
                
                # Procesar datos
                processed_df = process_stock_data(df, specific_ticker)
                
                # Guardar datos procesados
                processed_path = os.path.join(data_path, 'processed', f'{specific_ticker}_daily_{execution_date.strftime("%Y%m%d")}.csv')
                processed_df.to_csv(processed_path)
                
                return processed_path
            return process_daily_data

    # Tarea para procesar datos
    process_task = PythonOperator(
        task_id=f'process_{ticker.replace(".", "_")}',
        python_callable=create_process_function(ticker),
        op_kwargs={
            'data_path': data_path,
            'execution_date': '{{ execution_date }}'
        },
        provide_context=True,
        dag=dag,
    )
    process_tasks[ticker] = process_task

        # Definir función de carga para este ticker específico
    def create_load_function(specific_ticker):
        def load_to_database(**kwargs):
            ti = kwargs['ti']
            processed_path = ti.xcom_pull(task_ids=f'process_{specific_ticker.replace(".", "_")}')
            db_path, _ = ti.xcom_pull(task_ids='check_database')
            
            # Si no hay datos procesados, salir
            if not processed_path:
                return f"No hay datos nuevos para {specific_ticker}"
            
            # Cargar datos procesados
            df = pd.read_csv(processed_path, index_col=0, parse_dates=True)
            
            # Insertar en la base de datos
            conn = create_db_connection(db_path)
            save_to_database(conn, df, specific_ticker, 'daily')
            conn.close()
            
            return f"Datos diarios de {specific_ticker} cargados en la base de datos"
        return load_to_database

    # Tarea para cargar datos en la base de datos
    load_task = PythonOperator(
        task_id=f'load_{ticker.replace(".", "_")}',
        python_callable=create_load_function(ticker),
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
'data_path': data_path,
'execution_date': '{{ execution_date }}'
},
provide_context=True,
dag=dag,
)

# Definir dependencias
wait_for_market_close >> check_db

for ticker in IBEX35_TOP10:
    check_db >> download_tasks[ticker] >> process_tasks[ticker] >> load_tasks[ticker] >> update_summary_task