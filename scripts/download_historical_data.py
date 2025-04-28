# scripts/download_historical_data.py
#!/usr/bin/env python3
import argparse
import logging
import yaml
import sys
import os
from pathlib import Path
from datetime import datetime

# Añadir directorio raíz del proyecto al path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.etl.extraction.historical_extraction import HistoricalDataExtractor
from src.config.config import get_config, get_symbols

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

def main():
    """Función principal del script."""
    parser = argparse.ArgumentParser(description='Descarga datos históricos de acciones')
    
    # Argumentos de configuración
    parser.add_argument('--config', '-c', help='Ruta al archivo de configuración')
    parser.add_argument('--symbols', '-s', nargs='+', help='Lista de símbolos a descargar')
    parser.add_argument('--output-dir', '-o', help='Directorio de salida')
    parser.add_argument('--period', '-p', default='1y', help='Período (1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max)')
    parser.add_argument('--interval', '-i', default='1d', help='Intervalo (1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo)')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       help='Nivel de logging')
    parser.add_argument('--log-file', help='Archivo de log')
    parser.add_argument('--batch-size', '-b', type=int, default=5, help='Tamaño de lote para procesar símbolos')
    parser.add_argument('--max-workers', '-w', type=int, default=3, help='Máximo de trabajadores concurrentes')
    parser.add_argument('--delay', '-d', type=int, default=5, help='Retraso entre lotes (segundos)')
    
    args = parser.parse_args()
    
    # Cargar configuración
    config = get_config(args.config)
    
    # Configurar logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join(config['general']['data_path'], 'logs')
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = args.log_file or os.path.join(log_dir, f"historical_data_{timestamp}.log")
    setup_logging(args.log_level, log_file)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Iniciando descarga de datos históricos ({timestamp})")
    
    # Cargar símbolos (prioridad: argumentos CLI > configuración)
    symbols = args.symbols if args.symbols else get_symbols(args.config)
    logger.info(f"Procesando {len(symbols)} símbolos: {symbols}")
    
    # Determinar directorio de salida
    output_dir = args.output_dir or os.path.join(config['general']['data_path'], 'raw')
    
    # Configurar extractor
    extractor_config = {
        'max_workers': args.max_workers,
        'delay_between_batches': args.delay,
        'period': args.period or config.get('visualization', {}).get('default_period', '1y'),
        'interval': args.interval
    }
    
    extractor = HistoricalDataExtractor(symbols, output_dir, extractor_config)
    
    # Ejecutar extracción
    results = extractor.process_symbols_in_batches(args.batch_size)
    
    # Guardar informe
    report_dir = os.path.join(config['general']['data_path'], 'reports')
    Path(report_dir).mkdir(parents=True, exist_ok=True)
    report_path = extractor.save_results_report(results, os.path.join(report_dir, f"extraction_report_{timestamp}.csv"))
    
    # Mostrar resumen
    logger.info(f"Extracción completada: {results['successful']}/{results['total']} símbolos procesados correctamente")
    logger.info(f"Informe guardado en: {report_path}")
    
    # Devolver código de salida basado en éxito
    return 0 if results['failed'] == 0 else 1

if __name__ == "__main__":
    sys.exit(main())