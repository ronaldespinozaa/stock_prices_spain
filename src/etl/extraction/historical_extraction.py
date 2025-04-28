# src/etl/extraction/historical_extraction.py
import logging
import time
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from src.utils.stock_utils import download_stock_data, save_stock_data, validate_stock_data
from src.config.config import get_config

logger = logging.getLogger(__name__)

class HistoricalDataExtractor:
    """
    Extractor de datos históricos de acciones con mecanismos de control de rate limiting
    y procesamiento paralelizado controlado.
    """
    
    def __init__(self, tickers, output_dir, config=None):
        """
        Inicializa el extractor.
        
        Args:
            tickers (list): Lista de tickers a extraer
            output_dir (str): Directorio de salida para los datos
            config (dict, optional): Configuración adicional
        """
        self.tickers = tickers
        self.output_dir = output_dir
        self.config = config or {}
        
        # Configurar parámetros
        self.max_workers = self.config.get('max_workers', 3)  # Limitar paralelismo
        self.delay_between_batches = self.config.get('delay_between_batches', 5)
        self.period = self.config.get('period', '5y')  # Por defecto 5 años
        self.interval = self.config.get('interval', '1d')  # Por defecto diario
        
        # Crear directorio de salida
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Inicializado extractor para {len(tickers)} tickers, "
                   f"max_workers={self.max_workers}, period={self.period}")
    
    def _process_ticker(self, ticker):
        """
        Procesa un ticker individual.
        
        Args:
            ticker (str): Ticker a procesar
            
        Returns:
            dict: Resultado del procesamiento con metadatos
        """
        start_time = time.time()
        result = {
            'ticker': ticker,
            'success': False,
            'file_path': None,
            'rows': 0,
            'duration': 0
        }
        
        try:
            # Descargar datos
            data = download_stock_data(ticker, period=self.period, interval=self.interval)
            
            # Guardar datos
            output_path = save_stock_data(data, ticker, self.output_dir)
            result['file_path'] = output_path
            
            # Validar datos
            is_valid = validate_stock_data(output_path)
            result['success'] = is_valid
            
            if not data.empty:
                result['rows'] = len(data)
            
        except Exception as e:
            logger.error(f"Error al procesar {ticker}: {str(e)}")
            result['error'] = str(e)
        
        # Calcular duración
        result['duration'] = time.time() - start_time
        
        return result
    
    def process_tickers_in_batches(self, batch_size=5):
        """
        Procesa los símbolos en lotes para evitar rate limiting.
        
        Args:
            batch_size (int): Tamaño del lote
            
        Returns:
            dict: Resultados del procesamiento
        """
        results = []
        all_tickers = self.tickers
        
        
        logger.info(f"Procesando {len(self.tickers)} en lotes de {batch_size}")
        
        # Procesar en lotes
        for i in range(0, len(all_tickers), batch_size):
            batch = all_tickers[i:i+batch_size]
            logger.info(f"Procesando lote {i//batch_size + 1}: {batch}")
            
            # Procesar lote en paralelo con ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=min(self.max_workers, len(batch))) as executor:
                batch_results = list(executor.map(self._process_ticker, batch))
                results.extend(batch_results)
            
            # Esperar entre lotes para reducir carga en la API
            if i + batch_size < len(all_tickers):
                delay = self.delay_between_batches
                logger.info(f"Esperando {delay} segundos antes del siguiente lote...")
                time.sleep(delay)
        
        # Resumen de resultados
        successful = sum(1 for r in results if r['success'])
        logger.info(f"Procesamiento completado: {successful}/{len(results)} símbolos extraídos correctamente")
        
        return {
            'total': len(results),
            'successful': successful,
            'failed': len(results) - successful,
            'details': results
        }
    
    def save_results_report(self, results, output_file='extraction_report.csv'):
        """
        Guarda un informe de los resultados de la extracción.
        
        Args:
            results (dict): Resultados de la extracción
            output_file (str): Nombre del archivo de informe
            
        Returns:
            str: Ruta al archivo de informe
        """
        report_path = Path(self.output_dir) / output_file
        
        # Crear DataFrame con detalles
        report_data = pd.DataFrame(results['details'])
        
        # Guardar informe
        report_data.to_csv(report_path, index=False)
        logger.info(f"Informe de extracción guardado en {report_path}")
        
        return str(report_path)