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

def create_database(conn):
    """
    Crea las tablas necesarias en la base de datos si no existen
    
    Args:
        conn: Objeto de conexión a la base de datos SQLite
    """
    cursor = conn.cursor()
    
    # Crear tabla para datos históricos
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS historical_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        date TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Crear tabla para datos diarios
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        date TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()

def save_to_database(conn, df, ticker, data_type):
    """
    Guarda los datos en la base de datos

    Args:
        conn (sqlite3.Connection): Conexión a la base de datos SQLite
        df (DataFrame): DataFrame con los datos a guardar
        ticker (str): Símbolo de la acción
        data_type (str): Tipo de datos ('historical' o 'daily')

    Returns:
        bool: True si se guardaron los datos correctamente
    """
    try:
        # Preparar datos para la base de datos
        df = df.copy()  # Para evitar SettingWithCopyWarning
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'date'}, inplace=True)
        
        # Asegurar que la fecha esté en formato string para SQLite
        if pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # Añadir columna ticker si no existe
        if 'ticker' not in df.columns:
            df['ticker'] = ticker

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

        return True

    except Exception as e:
        print(f"Error al guardar datos en la base de datos: {str(e)}")
        # En un entorno de producción, es mejor usar un logger configurado
        # logger.error(f"Error al guardar datos en la base de datos: {str(e)}")
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

        # Validar que las fechas estén en el formato correcto
        start_date_str = start_date
        end_date_str = end_date
        
        # Si las fechas son objetos datetime, convertirlas a string
        if hasattr(start_date, 'strftime'):
            start_date_str = start_date.strftime('%Y-%m-%d')
        if hasattr(end_date, 'strftime'):
            end_date_str = end_date.strftime('%Y-%m-%d')

        # Consulta SQL
        query = f'''
        SELECT *
        FROM {table_name}
        WHERE ticker = ? AND date BETWEEN ? AND ?
        ORDER BY date
        '''

        # Ejecutar consulta
        df = pd.read_sql_query(query, conn, params=(ticker, start_date_str, end_date_str))

        # Convertir fecha a índice
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # Eliminar columnas innecesarias si existen
            columns_to_drop = ['id', 'created_at']
            for col in columns_to_drop:
                if col in df.columns:
                    df.drop(columns=[col], inplace=True)

        return df

    except Exception as e:
        print(f"Error al obtener datos de {ticker}: {str(e)}")
        # En un entorno de producción, usar un logger configurado:
        # logger.error(f"Error al obtener datos de {ticker}: {str(e)}")
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
            
            # Verificar si los datos tienen la columna necesaria
            if not df.empty and 'close' in df.columns:
                # Calcular retornos diarios si no existen
                if 'daily_return' not in df.columns:
                    df['daily_return'] = df['close'].pct_change()
                
                portfolio_data[ticker] = df

        if not portfolio_data:
            print("No hay datos disponibles para los tickers solicitados en el rango de fechas especificado")
            return None

        # Calcular retornos diarios
        returns_df = pd.DataFrame()

        for ticker, df in portfolio_data.items():
            returns_df[ticker] = df['daily_return']

        # Calcular matriz de correlación
        corr_matrix = returns_df.corr()

        # Calcular rendimiento acumulado
        cumulative_returns = {}
        annual_returns = {}
        trading_days_per_year = 252

        for ticker, df in portfolio_data.items():
            # Rendimiento acumulado total
            cumulative_returns[ticker] = (1 + df['daily_return'].fillna(0)).cumprod().iloc[-1] - 1
            
            # Rendimiento anualizado
            days_in_sample = len(df)
            if days_in_sample > 0:
                annual_returns[ticker] = (1 + cumulative_returns[ticker]) ** (trading_days_per_year / days_in_sample) - 1
            else:
                annual_returns[ticker] = 0

        # Calcular volatilidad anualizada
        volatility = {}

        for ticker, df in portfolio_data.items():
            volatility[ticker] = df['daily_return'].std() * (trading_days_per_year ** 0.5)

        # Calcular ratio de Sharpe (asumiendo tasa libre de riesgo de 0%)
        sharpe_ratio = {}
        risk_free_rate = 0.03  # 3% como ejemplo, ajustar según el mercado actual

        for ticker in portfolio_data.keys():
            if volatility[ticker] > 0:
                # Usando rendimiento anualizado para el Sharpe ratio
                sharpe_ratio[ticker] = (annual_returns[ticker] - risk_free_rate) / volatility[ticker]
            else:
                sharpe_ratio[ticker] = 0
        
        # Calcular drawdown máximo
        max_drawdown = {}
        
        for ticker, df in portfolio_data.items():
            # Calcular pico acumulativo
            cum_returns = (1 + df['daily_return'].fillna(0)).cumprod()
            running_max = cum_returns.cummax()
            drawdown = (cum_returns / running_max) - 1
            max_drawdown[ticker] = drawdown.min()

        # Crear resumen
        summary = {
            'cumulative_returns': cumulative_returns,
            'annual_returns': annual_returns,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'correlation_matrix': corr_matrix.to_dict()  # Convertir a dict para serialización más fácil
        }

        return summary

    except Exception as e:
        print(f"Error al calcular métricas del portafolio: {str(e)}")
        # En un entorno de producción:
        # logger.error(f"Error al calcular métricas del portafolio: {str(e)}")
        return None