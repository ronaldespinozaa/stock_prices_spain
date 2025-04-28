# scripts/run_complete_etl.py
#!/usr/bin/env python3
import argparse
import logging
import sys
import os
from pathlib import Path
from datetime import datetime

# Añadir directorio raíz del proyecto al path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.etl.extraction.historical_extraction import HistoricalDataExtractor
from src.etl.processing.data_processor import StockDataProcessor
from src.etl.loading.db_loader import StockDatabaseLoader
from src.config.config import get_config, get_tickers

# Configurar logging
def setup_logging(log_level="INFO", log_file=None):
    """Configura el sistema de logging."""
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Crear handler para consola
    handlers = [logging.StreamHandler()]
    
    # Añadir handler para archivo si se especifica
    if log_file:
        log_dir = Path(log_file).parent
        if not log_dir.exists():
            log_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    # Configurar logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=handlers
    )

def run_extraction(config, args):
    """
    Ejecuta la fase de extracción del ETL.
    
    Args:
        config (dict): Configuración del proyecto
        args (Namespace): Argumentos del script
        
    Returns:
        dict: Resultados de la extracción
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando fase de EXTRACCIÓN")
    
    # Cargar símbolos (prioridad: argumentos CLI > configuración)
    tickers = args.tickers if args.tickers else get_tickers(args.config)
    logger.info(f"Procesando {len(tickers)} símbolos: {tickers}")
    
    # Determinar directorio de salida
    output_dir = args.output_dir or os.path.join(config['general']['data_path'], 'raw')
    
    # Configurar extractor
    extractor_config = {
        'max_workers': args.max_workers,
        'delay_between_batches': args.delay,
        'period': args.period or config.get('visualization', {}).get('default_period', '1y'),
        'interval': args.interval
    }
    
    extractor = HistoricalDataExtractor(tickers, output_dir, extractor_config)
    
    # Ejecutar extracción
    results = extractor.process_tickers_in_batches(args.batch_size)
    
    # Guardar informe
    report_dir = os.path.join(config['general']['data_path'], 'reports')
    Path(report_dir).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = extractor.save_results_report(results, os.path.join(report_dir, f"extraction_report_{timestamp}.csv"))
    
    # Mostrar resumen
    logger.info(f"Extracción completada: {results['successful']}/{results['total']} símbolos procesados correctamente")
    logger.info(f"Informe guardado en: {report_path}")
    
    return results

def run_processing(config, args):
    """
    Ejecuta la fase de procesamiento del ETL.
    
    Args:
        config (dict): Configuración del proyecto
        args (Namespace): Argumentos del script
        
    Returns:
        dict: Resultados del procesamiento
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando fase de PROCESAMIENTO")
    
    # Determinar directorios
    raw_dir = os.path.join(config['general']['data_path'], 'raw')
    processed_dir = os.path.join(config['general']['data_path'], 'processed')
    
    # Crear procesador
    processor = StockDataProcessor(raw_dir, processed_dir, config)
    
    # Ejecutar procesamiento
    results = processor.process_all_files()
    
    # Mostrar resumen
    logger.info(f"Procesamiento completado: {results['successful']}/{results['total']} archivos procesados")
    
    return results

def run_loading(config, args):
    """
    Ejecuta la fase de carga en base de datos del ETL.
    
    Args:
        config (dict): Configuración del proyecto
        args (Namespace): Argumentos del script
        
    Returns:
        dict: Resultados de la carga
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando fase de CARGA EN BASE DE DATOS")
    
    # Determinar ruta de base de datos y directorio de datos procesados
    db_path = config['database']['path']
    processed_dir = os.path.join(config['general']['data_path'], 'processed')
    
    # Crear cargador de base de datos
    loader = StockDatabaseLoader(db_path, config)
    
    # Ejecutar carga
    results = loader.load_all_processed_files(processed_dir)
    
    # Mostrar resumen
    logger.info(f"Carga en base de datos completada: {results['successful']}/{results['total']} archivos cargados")
    
    return results

def main():
    """Función principal del script."""
    parser = argparse.ArgumentParser(description='ETL completo de datos históricos de acciones')
    
    # Argumentos generales
    parser.add_argument('--config', '-c', help='Ruta al archivo de configuración')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       help='Nivel de logging')
    parser.add_argument('--log-file', help='Archivo de log')
    parser.add_argument('--skip-extraction', action='store_true', help='Omitir la fase de extracción')
    parser.add_argument('--skip-processing', action='store_true', help='Omitir la fase de procesamiento')
    parser.add_argument('--skip-loading', action='store_true', help='Omitir la fase de carga en base de datos')
    
    # Argumentos específicos para extracción
    extraction_group = parser.add_argument_group('Extracción')
    extraction_group.add_argument('--tickers', '-s', nargs='+', help='Lista de tickers a descargar')
    extraction_group.add_argument('--output-dir', '-o', help='Directorio de salida para datos crudos')
    extraction_group.add_argument('--period', '-p', help='Período (1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max)')
    extraction_group.add_argument('--interval', '-i', default='1d', help='Intervalo (1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo)')
    extraction_group.add_argument('--batch-size', '-b', type=int, default=5, help='Tamaño de lote para procesar símbolos')
    extraction_group.add_argument('--max-workers', '-w', type=int, default=3, help='Máximo de trabajadores concurrentes')
    extraction_group.add_argument('--delay', '-d', type=int, default=5, help='Retraso entre lotes (segundos)')
    
    args = parser.parse_args()
    
    # Cargar configuración
    config = get_config(args.config)
    
    # Configurar logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join(config['general']['data_path'], 'logs')
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = args.log_file or os.path.join(log_dir, f"etl_complete_{timestamp}.log")
    setup_logging(args.log_level, log_file)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Iniciando ETL completo ({timestamp})")
    
    try:
        # 1. Extracción
        if not args.skip_extraction:
            extraction_results = run_extraction(config, args)
        else:
            logger.info("Fase de EXTRACCIÓN omitida")
        
        # 2. Procesamiento
        if not args.skip_processing:
            processing_results = run_processing(config, args)
        else:
            logger.info("Fase de PROCESAMIENTO omitida")
        
        # 3. Carga en base de datos
        if not args.skip_loading:
            loading_results = run_loading(config, args)
        else:
            logger.info("Fase de CARGA EN BASE DE DATOS omitida")
        
        logger.info("ETL completo finalizado con éxito")
        return 0
        
    except Exception as e:
        logger.error(f"Error en el ETL completo: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())