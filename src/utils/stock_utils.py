# src/utils/stock_utils.py
import yfinance as yf
import pandas as pd
import logging
import time
import random
from pathlib import Path
from src.utils.retry_utils import retry_with_exponential_backoff

logger = logging.getLogger(__name__)

@retry_with_exponential_backoff(max_retries=3, initial_delay=10)
def download_stock_data(ticker, period='5y', interval='1d'):
    """
    Descarga datos históricos para un ticker específico con reintentos automáticos.
    
    Args:
        ticker (str): Símbolo de la acción
        period (str): Período de tiempo ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
        interval (str): Intervalo de tiempo ('1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo')
        
    Returns:
        pandas.DataFrame: DataFrame con datos históricos
    """
    logger.info(f"Descargando datos para {ticker} con período {period} e intervalo {interval}")
    
    # Añadir pequeño retraso aleatorio para evitar rate limiting
    jitter = random.uniform(0.5, 2)
    time.sleep(jitter)
    
    # Intentar descargar los datos con yfinance
    data = yf.download(ticker, period=period, interval=interval, progress=False)
    
    if data.empty:
        logger.warning(f"No se encontraron datos para {ticker} (period={period}, interval={interval})")
        return pd.DataFrame()
    
    logger.info(f"Descargados {len(data)} registros para {ticker}")
    return data

def save_stock_data(data, ticker, output_dir, filename=None):
    """
    Guarda los datos de una acción en un archivo CSV con el formato específico requerido.
    
    Args:
        data (pandas.DataFrame): Datos a guardar
        ticker (str): Símbolo de la acción
        output_dir (str): Directorio de salida
        filename (str, optional): Nombre de archivo personalizado
        
    Returns:
        str: Ruta al archivo guardado
    """
    # Crear directorio si no existe
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Definir nombre de archivo
    if filename is None:
        filename = f"{ticker.replace('.', '_')}_historical.csv"
    
    output_path = Path(output_dir) / filename
    
    # Si los datos están vacíos, crear archivo con estructura predeterminada
    if data.empty:
        logger.warning(f"Guardando DataFrame vacío para {ticker} en {output_path}")
        
        # Crear DataFrame con el formato específico requerido
        empty_df = pd.DataFrame(columns=['Price', 'Close', 'High', 'Low', 'Open', 'Volume', 'Date'])
        
        # # Añadir fila con el ticker repetido
        # ticker_row = pd.DataFrame([['Ticker'] + [ticker] * 5], 
        #                        columns=['Price', 'Close', 'High', 'Low', 'Open', 'Volume'])
        
        # # Añadir fila de fecha (vacía)
        # date_row = pd.DataFrame([['Date', '', '', '', '', '']], 
        #                      columns=['Price', 'Close', 'High', 'Low', 'Open', 'Volume'])
        
        # Concatenar los DataFrames
        # result_df = pd.concat([empty_df.iloc[:0], ticker_row, date_row])
        
        # Guardar sin índice
        result_df.to_csv(output_path, index=False)
    else:
        logger.info(f"Guardando datos de {ticker} en {output_path}")
        
        # Convertir el DataFrame estándar de yfinance al formato específico requerido
        # Primero copiamos el DataFrame para no modificar el original
        processed_data = data.copy()
        
        # Crear un nuevo DataFrame con las columnas requeridas
        result_df = pd.DataFrame()
        
        # Mapear los datos de columnas
        result_df['Price'] = processed_data['Close']  # Usualmente 'Adj Close', pero depende de los datos exactos
        result_df['Close'] = processed_data['Close']
        result_df['High'] = processed_data['High']
        result_df['Low'] = processed_data['Low']
        result_df['Open'] = processed_data['Open']
        result_df['Volume'] = processed_data['Volume']
        result_df['Date'] = processed_data.index.strftime('%Y-%m-%d')  # Formatear la fecha


        
        # Crear DataFrame final
        # final_df = pd.concat([header_df, result_df.reset_index()])
        
        # Guardar sin índice y sin nombres de columnas (ya los incluimos manualmente)
        result_df.to_csv(output_path, index=False )
    
    return str(output_path)

def validate_stock_data(file_path):
    """
    Valida si un archivo CSV contiene datos válidos de acciones con el formato específico requerido.
    
    Args:
        file_path (str): Ruta al archivo CSV
        
    Returns:
        bool: True si los datos son válidos, False en caso contrario
    """
    try:
        path = Path(file_path)
        if not path.exists():
            logger.error(f"El archivo {file_path} no existe")
            return False
            
        if path.stat().st_size < 100:  # Verificar si el archivo es demasiado pequeño
            logger.warning(f"El archivo {file_path} es demasiado pequeño")
            return False
        
        # Leer las primeras líneas del archivo para validar la estructura
        with open(file_path, 'r') as f:
            # Leer las primeras 5 líneas
            lines = [next(f) for _ in range(5) if f]
        
        # Comprobar si tenemos suficientes líneas
        if len(lines) < 4:  # Necesitamos al menos los 3 encabezados + 1 fila de datos
            logger.warning(f"El archivo {file_path} no tiene suficientes líneas")
            return False
            
        # # Comprobar si las primeras líneas tienen el formato esperado
        # if 'Price' not in lines[0] or 'Ticker' not in lines[1] or 'Date' not in lines[2]:
        #     logger.warning(f"El archivo {file_path} no tiene el formato de encabezado esperado")
            # return False
            
        # Si llegamos aquí, el archivo parece tener la estructura básica correcta
        # Para validación adicional, podemos leer el archivo completo y verificar los datos
        df = pd.read_csv(file_path,)  # Saltar las 3 filas de encabezado
        
        # Verificar si hay al menos una fila de datos después de los encabezados
        if len(df) < 1:
            logger.warning(f"El archivo {file_path} no contiene datos después de los encabezados")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Error al validar {file_path}: {str(e)}")
        return False