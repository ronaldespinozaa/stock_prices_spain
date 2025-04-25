import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import logging
import os

import yfinance as yf
import pandas as pd
import logging

def download_stock_data(ticker, period):
    """
    Descarga datos históricos desde yfinance usando el parámetro 'period'.

    Args:
        ticker (str): Símbolo de la acción (ej. 'SAN.MC')
        period (str): Periodo de descarga (ej. '3y', 'max').
                      Valores válidos: '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'

    Returns:
        pandas.DataFrame: DataFrame con los datos históricos, o un DataFrame vacío en caso de error.
    """
    valid_periods = ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']
    if period not in valid_periods:
        logging.error(f"El periodo '{period}' no es válido para {ticker}.")
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

    try:
        df = yf.download(
            ticker,
            period=period,
            progress=False,
            threads=True,
            auto_adjust=True,
            timeout=30
        )

        if not df.empty:
            logging.info(f"Datos descargados correctamente para {ticker} con periodo '{period}'.Se han descargado {len(df)} filas.")
        else:
            logging.warning(f"No se encontraron datos para {ticker} con periodo '{period}'.")

        return df if not df.empty else pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

    except Exception as e:
        logging.error(f"Error al descargar datos para {ticker} con periodo '{period}': {e}")
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
    
def process_stock_data(df, ticker):
    """
Procesa y limpia los datos de una acción.

Args:
    df (pandas.DataFrame): DataFrame con datos crudos
    ticker (str): Símbolo de la acción
    
Returns:
    pandas.DataFrame: DataFrame procesado
"""
# Si el DataFrame está vacío, devolver un DataFrame vacío
    if df.empty:
        return pd.DataFrame()

    # Crear una copia para no modificar el original
    processed_df = df.copy()

    # Renombrar columnas
    processed_df.columns = [col.lower() for col in processed_df.columns]

    # Añadir columna de ticker
    processed_df['ticker'] = ticker

    # Añadir columna de fecha (útil cuando se convierte a formato tabular)
    processed_df['date'] = processed_df.index

    # Calcular retornos diarios
    processed_df['daily_return'] = processed_df['adj close'].pct_change()

    # Calcular indicadores técnicos básicos

    # Media móvil de 20 días
    processed_df['ma20'] = processed_df['close'].rolling(window=20).mean()

    # Media móvil de 50 días
    processed_df['ma50'] = processed_df['close'].rolling(window=50).mean()

    # Media móvil de 200 días
    processed_df['ma200'] = processed_df['close'].rolling(window=200).mean()

    # RSI (Relative Strength Index) de 14 días
    delta = processed_df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    processed_df['rsi14'] = 100 - (100 / (1 + rs))

    # MACD (Moving Average Convergence Divergence)
    exp1 = processed_df['close'].ewm(span=12, adjust=False).mean()
    exp2 = processed_df['close'].ewm(span=26, adjust=False).mean()
    processed_df['macd'] = exp1 - exp2
    processed_df['macd_signal'] = processed_df['macd'].ewm(span=9, adjust=False).mean()
    processed_df['macd_hist'] = processed_df['macd'] - processed_df['macd_signal']

    # Bollinger Bands
    processed_df['bb_middle'] = processed_df['close'].rolling(window=20).mean()
    processed_df['bb_std'] = processed_df['close'].rolling(window=20).std()
    processed_df['bb_upper'] = processed_df['bb_middle'] + (processed_df['bb_std'] * 2)
    processed_df['bb_lower'] = processed_df['bb_middle'] - (processed_df['bb_std'] * 2)

    # Limpiar valores NaN
    processed_df = processed_df.fillna(0)

    return processed_df
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
