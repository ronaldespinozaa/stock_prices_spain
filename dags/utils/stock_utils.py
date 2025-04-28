import yfinance as yf
import time
import logging
import pandas as pd
import numpy as np

import requests

logger = logging.getLogger(__name__)

def download_daily_data(tickers, start_date, end_date, max_reintentos=5, espera_base=5):
    data = {}

    for ticker in tickers:
        exito = False

        for intento in range(max_reintentos):
            try:
                print(f"Descargando datos para {ticker} (intento {intento + 1})...")
                df = yf.download(ticker, start=start_date, end=end_date, progress=False)

                if not df.empty:
                    data[ticker] = df
                    exito = True
                    break
                else:
                    raise ValueError("DataFrame vacío (posible rate limit o ticker incorrecto)")

            except (requests.exceptions.RequestException, ValueError) as e:
                espera = espera_base * (2 ** intento)
                print(f"Error al descargar {ticker}: {e}. Reintentando en {espera} segundos...")
                time.sleep(espera)

            except Exception as e:
                espera = espera_base * (2 ** intento)
                print(f"Error inesperado al descargar {ticker}: {e}. Reintentando en {espera} segundos...")
                time.sleep(espera)

        if not exito:
            print(f"No se pudo descargar {ticker} tras {max_reintentos} intentos.")

    return data
    
def process_stock_data(data_dict):
    """
    Procesa y limpia los datos de múltiples acciones.

    Args:
        data_dict (dict): Diccionario con tickers como claves y DataFrames crudos como valores

    Returns:
        pandas.DataFrame: DataFrame combinado y procesado
    """
    all_data = []

    for ticker, df in data_dict.items():
        # Si el DataFrame está vacío, continuar con el siguiente
        if df.empty:
            continue

        # Crear una copia para no modificar el original
        processed_df = df.copy()

        # Renombrar columnas
        processed_df.columns = [col.lower() for col in processed_df.columns]

        # Añadir columna de ticker y fecha
        processed_df['ticker'] = ticker
        processed_df['date'] = processed_df.index

        # Calcular retornos diarios
        processed_df['daily_return'] = processed_df['adj close'].pct_change()

        # Media móvil de 20, 50, 200 días
        processed_df['ma20'] = processed_df['close'].rolling(window=20).mean()
        processed_df['ma50'] = processed_df['close'].rolling(window=50).mean()
        processed_df['ma200'] = processed_df['close'].rolling(window=200).mean()

        # RSI de 14 días
        delta = processed_df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(window=14).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
        rs = gain / loss
        processed_df['rsi14'] = 100 - (100 / (1 + rs))

        # MACD
        exp1 = processed_df['close'].ewm(span=12, adjust=False).mean()
        exp2 = processed_df['close'].ewm(span=26, adjust=False).mean()
        processed_df['macd'] = exp1 - exp2
        processed_df['macd_signal'] = processed_df['macd'].ewm(span=9, adjust=False).mean()
        processed_df['macd_hist'] = processed_df['macd'] - processed_df['macd_signal']

        # Bollinger Bands
        processed_df['bb_middle'] = processed_df['close'].rolling(window=20).mean()
        processed_df['bb_std'] = processed_df['close'].rolling(window=20).std()
        processed_df['bb_upper'] = processed_df['bb_middle'] + 2 * processed_df['bb_std']
        processed_df['bb_lower'] = processed_df['bb_middle'] - 2 * processed_df['bb_std']

        # Limpiar valores NaN
        processed_df = processed_df.fillna(0)

        # Añadir al conjunto de datos procesados
        all_data.append(processed_df)

    # Unir todos los DataFrames
    final_df = pd.concat(all_data, ignore_index=True)

    return final_df

def calculate_portfolio_metrics(df_dict, weights=None):
    """
    Calcula métricas para un portafolio de acciones.

    Args:
        df_dict (dict): Diccionario con DataFrames de acciones {ticker: df}
        weights (dict, optional): Pesos de cada acción en el portafolio {ticker: peso}

    Returns:
        pandas.DataFrame: DataFrame con métricas del portafolio
    """
    # Si no se proporcionan pesos, usar pesos iguales
    if weights is None:
        n = len(df_dict)
        weights = {ticker: 1/n for ticker in df_dict.keys()}

    # Crear DataFrame para retornos
    returns_df = pd.DataFrame()

    # Añadir retornos de cada acción
    for ticker, df in df_dict.items():
        if not df.empty and 'daily_return' in df.columns:
            returns_df[ticker] = df['daily_return']

    # Si no hay datos de retornos, devolver DataFrame vacío
    if returns_df.empty:
        return pd.DataFrame()

    # Calcular retorno del portafolio
    portfolio_return = pd.Series(0, index=returns_df.index)
    for ticker in returns_df.columns:
        portfolio_return += returns_df[ticker] * weights.get(ticker, 0)

    # Crear DataFrame de métricas
    metrics_df = pd.DataFrame(index=returns_df.index)
    metrics_df['portfolio_return'] = portfolio_return

    # Calcular retorno acumulado
    metrics_df['cumulative_return'] = (1 + metrics_df['portfolio_return']).cumprod() - 1

    # Calcular volatilidad móvil (20 días)
    metrics_df['volatility_20d'] = metrics_df['portfolio_return'].rolling(window=20).std() * np.sqrt(252)

    # Calcular ratio de Sharpe móvil (20 días, asumiendo tasa libre de riesgo de 0%)
    metrics_df['sharpe_ratio_20d'] = (metrics_df['portfolio_return'].rolling(window=20).mean() * 252) / metrics_df['volatility_20d']

    # Calcular drawdown
    rolling_max = metrics_df['cumulative_return'].rolling(window=252, min_periods=1).max()
    metrics_df['drawdown'] = metrics_df['cumulative_return'] - rolling_max

    # Limpiar valores NaN
    metrics_df = metrics_df.fillna(0)

    return metrics_df

