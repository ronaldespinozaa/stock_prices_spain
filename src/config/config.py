# src/config/config.py
import os
import yaml
from pathlib import Path

# Ruta base del proyecto (2 niveles arriba desde este archivo)
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Rutas predeterminadas
DEFAULT_CONFIG_FILE = BASE_DIR / "config.yaml"

def get_config(config_file=None, env=None):
    """
    Carga la configuración del proyecto.
    
    Args:
        config_file (str, optional): Ruta al archivo de configuración principal
        env (str, optional): Entorno a utilizar (dev, test, prod)
        
    Returns:
        dict: Configuración cargada
    """
    # Usar archivo de configuración predeterminado si no se especifica
    config_file = config_file or os.environ.get("CONFIG_FILE", DEFAULT_CONFIG_FILE)
    
    # Determinar entorno
    env = env or os.environ.get("ENV", "dev")
    
    # Configuración predeterminada
    config = {
        "environment": env,
        "general": {
            "data_path": str(BASE_DIR / "data")
        },
        "extraction": {
            "max_workers": 3,
            "batch_size": 5,
            "delay_between_batches": 5,
            "retries": 3,
            "default_period": "1y",
            "default_interval": "1d"
        },
        "database": {
            "path": str(BASE_DIR / "data/database/stocks.db"),
            "type": "sqlite"
        }
    }
    
    # Cargar configuración desde archivo si existe
    if os.path.exists(config_file):
        try:
            with open(config_file, "r") as f:
                file_config = yaml.safe_load(f)
                if file_config:
                    # Actualizar recursivamente
                    _update_dict_recursive(config, file_config)
        except Exception as e:
            print(f"Error al cargar configuración desde {config_file}: {str(e)}")
    
    return config

def _update_dict_recursive(target, source):
    """
    Actualiza un diccionario de forma recursiva.
    
    Args:
        target (dict): Diccionario a actualizar
        source (dict): Diccionario con valores nuevos
    """
    for key, value in source.items():
        if key in target and isinstance(target[key], dict) and isinstance(value, dict):
            _update_dict_recursive(target[key], value)
        else:
            target[key] = value
            
def get_tickers(config_file=None):
    """
    Carga los tickers desde la configuración.
    
    Args:
        config_file (str, optional): Ruta al archivo de configuración
        
    Returns:
        list: Lista de símbolos
    """
    config = get_config(config_file)
    
    # Intentar obtener los tickers de la configuración
    try:
        # Primero buscar en la estructura que tienes en tu configuración actual
        if 'stocks' in config and 'tickers' in config['stocks']:
            return config['stocks']['tickers']
    except Exception as e:
        print(f"Error al obtener tickers de la configuración: {str(e)}")
    
    # Valores predeterminados del IBEX 35 si no se puede cargar de la configuración
    return [
        "SAN.MC", "BBVA.MC", "IBE.MC", "ITX.MC", "TEF.MC", 
        "REP.MC", "CABK.MC", "NTGY.MC", "FER.MC", "AMS.MC"
    ]