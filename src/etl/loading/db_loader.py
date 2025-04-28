# src/etl/loading/db_loader.py
import pandas as pd
import sqlite3
import logging
from pathlib import Path
import glob
import os

logger = logging.getLogger(__name__)

class StockDatabaseLoader:
    """
    Clase para cargar datos procesados de acciones en una base de datos.
    """
    
    def __init__(self, db_path, config=None):
        """
        Inicializa el cargador de base de datos.
        
        Args:
            db_path (str): Ruta a la base de datos SQLite
            config (dict, optional): Configuración adicional
        """
        self.db_path = Path(db_path)
        self.config = config or {}
        
        # Crear directorio para la base de datos si no existe
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Inicializado cargador de base de datos: {db_path}")
    
    def _get_connection(self):
        """
        Establece conexión con la base de datos.
        
        Returns:
            sqlite3.Connection: Conexión a la base de datos
        """
        try:
            conn = sqlite3.connect(self.db_path)
            return conn
        except sqlite3.Error as e:
            logger.error(f"Error al conectar con la base de datos: {str(e)}")
            raise
    
    def initialize_db(self):
        """
        Inicializa la estructura de la base de datos si no existe.
        
        Returns:
            bool: True si la inicialización fue exitosa
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Crear tabla de acciones
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                price REAL,
                volume INTEGER,
                daily_return REAL,
                sma_20 REAL,
                ema_20 REAL,
                sma_50 REAL,
                volatility_20d REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, date)
            )
            ''')
            
            # Crear índices para optimizar consultas comunes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_ticker ON stocks (ticker)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_date ON stocks (date)')
            
            conn.commit()
            conn.close()
            
            logger.info("Base de datos inicializada correctamente")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error al inicializar la base de datos: {str(e)}")
            return False
    
    def load_dataframe(self, df, ticker, replace=True):
        """
        Carga un DataFrame en la base de datos.
        
        Args:
            df (pandas.DataFrame): DataFrame con datos procesados
            ticker (str): Símbolo de la acción
            replace (bool): Si se deben reemplazar datos existentes
            
        Returns:
            dict: Resultado de la carga
        """
        if df.empty:
            logger.warning(f"DataFrame vacío para {ticker}, no hay nada que cargar")
            return {'ticker': ticker, 'loaded': 0, 'error': "DataFrame vacío"}
        
        db_data = pd.DataFrame()
        try:
            # Preparar DataFrame para la carga
            # Resetear índice para tener la fecha como columna
            load_df = df.reset_index(drop=True) if 'Date' not in df.columns else df.copy()
            
            # Asegurarse de que la columna de fecha es de tipo string en formato YYYY-MM-DD
            if 'Date' in load_df.columns:
                load_df['Date'] = pd.to_datetime(load_df['Date']).dt.strftime('%Y-%m-%d')
            
            # Establecer conexión
            conn = self._get_connection()
            
            # Determinar modo de carga
            if replace:
                # Eliminar datos existentes para este ticker
                cursor = conn.cursor()
                cursor.execute("DELETE FROM stocks WHERE ticker = ?", (ticker,)) #
                # logger.info(f"Cargados {len(db_data)} registros para {ticker} en la base de datos")

                conn.commit()
            
            # Mapear columnas del DataFrame a columnas de la base de datos
            # Crear un nuevo DataFrame con las columnas necesarias para la base de datos

            
            # Mapeo de columnas (DataFrame -> DB)
            column_mapping = {
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Price': 'price',
                'Volume': 'volume',
                'Daily_Return': 'daily_return',
                'SMA_20': 'sma_20',
                'EMA_20': 'ema_20',
                'SMA_50': 'sma_50',
                'Volatility_20d': 'volatility_20d'
            }
            
            # Añadir ticker
            db_data['ticker'] = [ticker] * len(load_df)
            
            # Mapear columnas
            for df_col, db_col in column_mapping.items():
                if df_col in load_df.columns:
                    db_data[db_col] = load_df[df_col]
                else:
                    db_data[db_col] = None

            # Cargar datos en la base de datos
            if db_data.empty:
                logger.warning(f"No hay datos válidos para cargar en la base de datos para {ticker}")
                return {'ticker': ticker, 'loaded': 0, 'success': False, 'error': "No hay datos válidos para cargar"}

            db_data.to_sql('stocks', conn, if_exists='append', index=False)
            
            # Cerrar conexión
            conn.close()
            
            result = {
                'ticker': ticker,
                'loaded': len(db_data),
                'success': True
            }
            
            logger.info(f"Cargados {len(db_data)} registros para {ticker} en la base de datos")
            return result
            
        except Exception as e:
            logger.error(f"Error al cargar datos para {ticker} en la base de datos: {str(e)}")
            return {
                'ticker': ticker,
                'loaded': 0,
                'success': False,
                'error': str(e)
            }
    
    def load_from_csv(self, csv_path):
        """
        Carga datos desde un archivo CSV procesado.
        
        Args:
            csv_path (str): Ruta al archivo CSV procesado
            
        Returns:
            dict: Resultado de la carga
        """
        try:
            # Determinar ticker desde el nombre de archivo
            file_name = Path(csv_path).stem
            ticker_parts = file_name.split('_')
            
            if len(ticker_parts) < 2:
                logger.error(f"Formato de nombre de archivo no válido: {csv_path}")
                return {'file': csv_path, 'success': False, 'error': "Formato de nombre de archivo inválido"}
            
            ticker = ticker_parts[0]
            if '_' in ticker:
                ticker = ticker.replace('_', '.')
            
            # Cargar CSV
            df = pd.read_csv(csv_path)
            
            # Cargar en la base de datos
            result = self.load_dataframe(df, ticker)
            result['file'] = csv_path
            
            return result
            
        except Exception as e:
            logger.error(f"Error al cargar {csv_path}: {str(e)}")
            return {'file': csv_path, 'success': False, 'error': str(e)}
    
    def load_all_processed_files(self, processed_dir):
        """
        Carga todos los archivos procesados en la base de datos.
        
        Args:
            processed_dir (str): Directorio de datos procesados
            
        Returns:
            dict: Resultado de la carga
        """
        # Buscar archivos CSV procesados
        processed_files = glob.glob(str(Path(processed_dir) / "*_processed.csv"))
        logger.info(f"Encontrados {len(processed_files)} archivos procesados para cargar")
        
        results = {
            'total': len(processed_files),
            'successful': 0,
            'failed': 0,
            'files': []
        }
        
        # Inicializar base de datos
        if not self.initialize_db():
            logger.error("Error al inicializar la base de datos, abortando carga")
            results['error'] = "Error al inicializar la base de datos"
            return results
        
        # Cargar cada archivo
        for file_path in processed_files:
            result = self.load_from_csv(file_path)
            
            if result.get('success', False):
                results['successful'] += 1
            else:
                results['failed'] += 1
                
            results['files'].append(result)
        
        logger.info(f"Carga en base de datos completada: {results['successful']}/{results['total']} archivos cargados correctamente")
        return results