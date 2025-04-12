import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime, timedelta

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_db_connection(db_path):
    """
    Crea una conexión a la base de datos SQLite.

    Args:
        db_path (str): Ruta al archivo de base de datos

    Returns:
        sqlite3.Connection: Objeto de conexión a la base de datos
    """
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        return None

def create_database(config, **kwargs):
    """
    Crea la base de datos si no existe

    Args:
        config (dict): Configuración del proyecto

    Returns:
        str: Ruta a la base de datos
    """
    logger.info("Verificando base de datos")

    try:
        # Obtener ruta de la base de datos
        data_path = config['general']['data_path']
        db_dir = os.path.join(data_path, 'database')
        os.makedirs(db_dir, exist_ok=True)

        db_path = os.path.join(db_dir, 'stocks.db')

        # Conectar a la base de datos
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Crear tablas si no existen
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS historical_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            ticker TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            daily_return REAL,
            ma20 REAL,
            ma50 REAL,
            ma200 REAL,
            rsi14 REAL,
            macd REAL,
            macd_signal REAL,
            macd_hist REAL,
            bb_middle REAL,
            bb_upper REAL,
            bb_lower REAL,
            UNIQUE(date, ticker)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            ticker TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            daily_return REAL,
            ma20 REAL,
            ma50 REAL,
            ma200 REAL,
            rsi14 REAL,
            macd REAL,
            macd_signal REAL,
            macd_hist REAL,
            bb_middle REAL,
            bb_upper REAL,
            bb_lower REAL,
            UNIQUE(date, ticker)
        )
        ''')

        conn.commit()
        conn.close()

        logger.info(f"Base de datos verificada en {db_path}")

        return db_path

    except Exception as e:
        logger.error(f"Error al crear la base de datos: {str(e)}")
        raise

def save_to_database(ticker, data_type, config, **kwargs):
    """
    Guarda los datos en la base de datos

    Args:
        ticker (str): Símbolo de la acción
        data_type (str): Tipo de datos ('historical' o 'daily')
        config (dict): Configuración del proyecto

    Returns:
        bool: True si se guardaron los datos correctamente
    """
    logger.info(f"Guardando datos de {ticker} en la base de datos")

    try:
        # Cargar datos temporales
        data_path = config['general']['data_path']
        temp_dir = os.path.join(data_path, 'temp')
        file_path = os.path.join(temp_dir, f"{ticker}_{data_type}.csv")

        if not os.path.exists(file_path):
            logger.error(f"No se encontraron datos para {ticker}")
            return None

        df = pd.read_csv(file_path, index_col=0, parse_dates=True)

        # Preparar datos para la base de datos
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'date'}, inplace=True)
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        # Obtener ruta de la base de datos
        db_dir = os.path.join(data_path, 'database')
        db_path = os.path.join(db_dir, 'stocks.db')

        # Conectar a la base de datos
        conn = sqlite3.connect(db_path)

        # Determinar la tabla de destino
        table_name = f"{data_type}_data"

        # Insertar datos en la base de datos
        df.to_sql(table_name, conn, if_exists='append', index=False)

        # Eliminar duplicados
        cursor = conn.cursor()
        cursor.execute(f'''
        DELETE FROM {table_name}
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM {table_name}
            GROUP BY date, ticker
        )
        ''')

        conn.commit()
        conn.close()

        logger.info(f"Datos de {ticker} guardados en la base de datos")

        return True

    except Exception as e:
        logger.error(f"Error al guardar datos en la base de datos: {str(e)}")
        raise

def get_stock_data(conn, ticker, data_type, start_date, end_date):
    """
    Obtiene datos de una acción de la base de datos

    Args:
        conn (sqlite3.Connection): Conexión a la base de datos
        ticker (str): Símbolo de la acción
        data_type (str): Tipo de datos ('historical' o 'daily')
        start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'
        end_date (str): Fecha de fin en formato 'YYYY-MM-DD'

    Returns:
        pandas.DataFrame: DataFrame con los datos de la acción
    """
    try:
        # Determinar la tabla de origen
        table_name = f"{data_type}_data"

        # Consulta SQL
        query = f'''
        SELECT *
        FROM {table_name}
        WHERE ticker = ? AND date BETWEEN ? AND ?
        ORDER BY date
        '''

        # Ejecutar consulta
        df = pd.read_sql_query(query, conn, params=(ticker, start_date, end_date))

        # Convertir fecha a índice
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)

        return df

    except Exception as e:
        logger.error(f"Error al obtener datos de {ticker}: {str(e)}")
        return pd.DataFrame()

def get_portfolio_metrics(conn, tickers, start_date, end_date):
    """
    Calcula métricas para un portafolio de acciones

    Args:
        conn (sqlite3.Connection): Conexión a la base de datos
        tickers (list): Lista de símbolos de acciones
        start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'
        end_date (str): Fecha de fin en formato 'YYYY-MM-DD'

    Returns:
        dict: Diccionario con métricas del portafolio
    """
    try:
        # Obtener datos de todas las acciones
        portfolio_data = {}

        for ticker in tickers:
            df = get_stock_data(conn, ticker, 'historical', start_date, end_date)
            if not df.empty:
                portfolio_data[ticker] = df

        if not portfolio_data:
            return None

        # Calcular retornos diarios
        returns_df = pd.DataFrame()

        for ticker, df in portfolio_data.items():
            returns_df[ticker] = df['daily_return']

        # Calcular matriz de correlación
        corr_matrix = returns_df.corr()

        # Calcular rendimiento acumulado
        cumulative_returns = {}

        for ticker, df in portfolio_data.items():
            cumulative_returns[ticker] = (1 + df['daily_return'].fillna(0)).cumprod().iloc[-1] - 1

        # Calcular volatilidad anualizada
        volatility = {}

        for ticker, df in portfolio_data.items():
            volatility[ticker] = df['daily_return'].std() * (252 ** 0.5)

        # Calcular ratio de Sharpe (asumiendo tasa libre de riesgo de 0%)
        sharpe_ratio = {}

        for ticker in portfolio_data.keys():
            if volatility[ticker] > 0:
                sharpe_ratio[ticker] = cumulative_returns[ticker] / volatility[ticker]
            else:
                sharpe_ratio[ticker] = 0

        # Crear resumen
        summary = {
            'cumulative_returns': cumulative_returns,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'correlation_matrix': corr_matrix
        }

        return summary

    except Exception as e:
        logger.error(f"Error al calcular métricas del portafolio: {str(e)}")
        return None