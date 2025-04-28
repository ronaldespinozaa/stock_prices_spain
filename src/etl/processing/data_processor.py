# src/etl/processing/data_processor.py
import pandas as pd
import logging
from pathlib import Path
import os
import glob

logger = logging.getLogger(__name__)

class StockDataProcessor:
    """
    Clase para procesar datos crudos de acciones y prepararlos para almacenamiento.
    """
    
    def __init__(self, raw_dir, processed_dir, config=None):
        """
        Inicializa el procesador.
        
        Args:
            raw_dir (str): Directorio de datos crudos
            processed_dir (str): Directorio para datos procesados
            config (dict, optional): Configuración adicional
        """
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.config = config or {}
        
        # Crear directorio procesado si no existe
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Inicializado procesador de datos con directorio raw: {raw_dir}")
    
    def load_raw_data(self, file_path):
        """
        Carga datos crudos desde un archivo CSV con formato específico.
        
        Args:
            file_path (str): Ruta al archivo CSV
            
        Returns:
            tuple: (ticker, pandas.DataFrame)
        """
        try:
            # # Leer primero el ticker de la segunda línea
            # with open(file_path, 'r') as f:
            #     _ = f.readline()  # Saltar primera línea (encabezados)
            #     ticker_line = f.readline()
            #     ticker_parts = ticker_line.strip().split(',')
            #     if len(ticker_parts) > 1:
            #         ticker = ticker_parts[1]  # Obtener ticker de la segunda columna
            #     else:
            #         ticker = Path(file_path).stem.split('_')[0]  # Fallback al nombre de archivo
            #Leer el ticker del nombre del fichero dentro de la raw_path.Lo encuentras a la izquierda del segundo guion bajo
            ticker = Path(file_path).stem.split('_')[0]  # Fallback al nombre de archivo
            # Leer datos (saltando 3 líneas de encabezado)
            df = pd.read_csv(file_path )
            
            # # Si la primera columna es una fecha, configurarla como índice
            # if 'Unnamed: 0' in df.columns:
            #     df = df.rename(columns={'Unnamed: 0': 'Date'})
                
            # if 'Date' in df.columns:
            #     df['Date'] = pd.to_datetime(df['Date'])
            #     df = df.set_index('Date')
            
            # logger.info(f"Cargados {len(df)} registros para {ticker} desde {file_path}")
            return ticker, df
            
        except Exception as e:
            logger.error(f"Error al cargar datos desde {file_path}: {str(e)}")
            return None, pd.DataFrame()
    
    def process_stock_data(self, df, ticker):
        """
        Procesa los datos de una acción.
        
        Args:
            df (pandas.DataFrame): DataFrame con datos de la acción
            ticker (str): Símbolo de la acción
            
        Returns:
            pandas.DataFrame: DataFrame procesado
        """
        if df.empty:
            logger.warning(f"Sin datos para procesar para {ticker}")
            return df
        
        try:
            # Crear copia para no modificar el original
            processed_df = df.copy()
            
            # 1. Asegurar que tenemos todas las columnas necesarias
            required_columns = ['Price', 'Close', 'High', 'Low', 'Open', 'Volume','Date']
            for col in required_columns:
                if col not in processed_df.columns:
                    logger.warning(f"Columna {col} no encontrada para {ticker}, creando columna vacía")
                    processed_df[col] = None
            
            # 2. Calcular indicadores técnicos básicos
            
            # Movimientos promedio (si hay suficientes datos)
            if len(processed_df) >= 20:
                # Media móvil simple de 20 días
                processed_df['SMA_20'] = processed_df['Close'].rolling(window=20).mean()
                
                # Media móvil exponencial de 20 días
                processed_df['EMA_20'] = processed_df['Close'].ewm(span=20, adjust=False).mean()
            
            if len(processed_df) >= 50:
                # Media móvil simple de 50 días
                processed_df['SMA_50'] = processed_df['Close'].rolling(window=50).mean()
            
            # 3. Calcular rendimientos
            processed_df['Daily_Return'] = processed_df['Close'].pct_change() * 100
            
            # 4. Calcular volatilidad (desviación estándar móvil de 20 días de rendimientos diarios)
            if len(processed_df) >= 20:
                processed_df['Volatility_20d'] = processed_df['Daily_Return'].rolling(window=20).std()
            
            # 5. Añadir información de ticker para facilitar consultas en la base de datos
            processed_df['ticker'] = ticker
            
            # 6. Eliminar filas con valores NaN (opcional, dependiendo de tus necesidades)
            # processed_df = processed_df.dropna()
            
            logger.info(f"Datos procesados para {ticker}: {len(processed_df)} registros")
            return processed_df
            
        except Exception as e:
            logger.error(f"Error al procesar datos para {ticker}: {str(e)}")
            return df
    
    def save_processed_data(self, df, ticker):
        """
        Guarda los datos procesados en un archivo CSV.
        
        Args:
            df (pandas.DataFrame): DataFrame procesado
            ticker (str): Símbolo de la acción
            
        Returns:
            str: Ruta al archivo guardado
        """
        if df.empty:
            logger.warning(f"Sin datos para guardar para {ticker}")
            return None
        
        try:
            # Crear nombre de archivo para datos procesados
            filename = f"{ticker.replace('.', '_')}_processed.csv"
            output_path = self.processed_dir / filename
            
            # Guardar datos procesados
            df.to_csv(output_path)
            logger.info(f"Datos procesados guardados en {output_path}")
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error al guardar datos procesados para {ticker}: {str(e)}")
            return None
    
    def process_all_files(self):
        """
        Procesa todos los archivos crudos encontrados en el directorio de entrada.
        
        Returns:
            dict: Resultado del procesamiento
        """
        # Buscar archivos CSV en el directorio de datos crudos
        raw_files = glob.glob(str(self.raw_dir / "*_historical.csv"))
        logger.info(f"Encontrados {len(raw_files)} archivos para procesar")
        
        results = {
            'total': len(raw_files),
            'successful': 0,
            'failed': 0,
            'files': []
        }
        
        # Procesar cada archivo
        for file_path in raw_files:
            file_result = {
                'raw_file': file_path,
                'processed_file': None,
                'ticker': None,
                'success': False,
                'records': 0
            }
            
            try:
                # Cargar datos crudos
                ticker, raw_df = self.load_raw_data(file_path)
                file_result['ticker'] = ticker
                
                if raw_df.empty:
                    logger.warning(f"Archivo vacío o inválido: {file_path}")
                    results['failed'] += 1
                    file_result['error'] = "Archivo vacío o inválido"
                    results['files'].append(file_result)
                    continue
                
                # Procesar datos
                processed_df = self.process_stock_data(raw_df, ticker)
                
                # Guardar datos procesados
                output_path = self.save_processed_data(processed_df, ticker)
                
                if output_path:
                    file_result['processed_file'] = output_path
                    file_result['success'] = True
                    file_result['records'] = len(processed_df)
                    results['successful'] += 1
                else:
                    results['failed'] += 1
                    file_result['error'] = "Error al guardar datos procesados"
            
            except Exception as e:
                logger.error(f"Error al procesar {file_path}: {str(e)}")
                results['failed'] += 1
                file_result['error'] = str(e)
            
            results['files'].append(file_result)
        
        logger.info(f"Procesamiento completado: {results['successful']}/{results['total']} archivos procesados correctamente")
        return results