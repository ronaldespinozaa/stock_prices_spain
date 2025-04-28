import time
import random
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def retry_with_exponential_backoff(max_retries=5, initial_delay=5, exponential_base=2, jitter=True):
    """
    Decorador para reintentar funciones con backoff exponencial.
    
    Args:
        max_retries: Número máximo de reintentos
        initial_delay: Tiempo inicial de espera (segundos)
        exponential_base: Base para cálculo de espera exponencial
        jitter: Si se debe añadir variación aleatoria al tiempo de espera
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            num_retries = 0
            delay = initial_delay
            
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    num_retries += 1
                    if num_retries > max_retries:
                        logger.error(f"Máximo número de reintentos ({max_retries}) alcanzado. Error: {str(e)}")
                        raise
                    
                    # Calcular tiempo de espera con backoff exponencial
                    wait = delay * (exponential_base ** (num_retries - 1))
                    
                    # Añadir jitter si está activado
                    if jitter:
                        wait = wait * (0.5 + random.random())
                    
                    logger.warning(f"Error al ejecutar {func.__name__}: {str(e)}. "
                                  f"Reintentando en {wait:.2f} segundos. "
                                  f"Intento {num_retries} de {max_retries}.")
                    
                    time.sleep(wait)
        return wrapper
    return decorator