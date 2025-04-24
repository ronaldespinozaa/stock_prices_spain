import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
import os
import sys
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px

# Añadir directorio raíz al path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dags.utils.db_utils import create_db_connection, get_stock_data, get_portfolio_metrics

# Configuración de la página
st.set_page_config(
    page_title="Resumen del Mercado | ETL Acciones IBEX 35",
    page_icon="📊",
    layout="wide",
)

# Título de la página
st.title("📊 Resumen del Mercado Español")
st.markdown("Análisis general de las principales acciones del IBEX 35")

# Función para obtener la conexión a la base de datos
@st.cache_resource
def get_connection():
    # Ruta a la base de datos
    data_path = os.environ.get('DATA_PATH', os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data'))
    db_path = os.path.join(data_path, 'database', 'stocks.db')

    # Verificar si la base de datos existe
    if not os.path.exists(db_path):
        st.error(f"Base de datos no encontrada en {db_path}. Ejecuta primero los DAGs de Airflow.")
        return None

    # Crear conexión
    return create_db_connection(db_path)

# Obtener conexión
conn = get_connection()

if conn is None:
    st.stop()

# Función para obtener la lista de tickers disponibles
@st.cache_data(ttl=3600)
def get_available_tickers():
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT ticker FROM historical_data")
    tickers = [row[0] for row in cursor.fetchall()]
    return tickers

# Obtener tickers disponibles
tickers = get_available_tickers()

if not tickers:
    st.warning("No se encontraron datos de acciones en la base de datos. Ejecuta primero los DAGs de Airflow.")
    st.stop()

# Sidebar para filtros
st.sidebar.header("Filtros")

# Rango de fechas
# Obtener rango de fechas disponibles
cursor = conn.cursor()
cursor.execute("SELECT MIN(date), MAX(date) FROM historical_data")
min_date_str, max_date_str = cursor.fetchone()

min_date = datetime.strptime(min_date_str, '%Y-%m-%d') if min_date_str else datetime.now() - timedelta(days=365*2)
max_date = datetime.strptime(max_date_str, '%Y-%m-%d') if max_date_str else datetime.now()

# Selección de rango de fechas
date_range = st.sidebar.date_input(
    "Rango de Fechas",
    value=(max_date - timedelta(days=30), max_date),
    min_value=min_date,
    max_value=max_date
)

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = date_range[0]
    end_date = date_range[0]

# Convertir a formato string para consultas SQL
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Tipo de datos
data_type = st.sidebar.radio(
    "Tipo de Datos",
    options=["historical", "daily"],
    format_func=lambda x: "Históricos" if x == "historical" else "Diarios"
)

# Función para obtener datos de todas las acciones
@st.cache_data(ttl=3600)
def load_all_stock_data(tickers, data_type, start_date, end_date):
    data = {}
    for ticker in tickers:
        df = get_stock_data(conn, ticker, data_type, start_date, end_date)
        if not df.empty:
            data[ticker] = df
    return data

# Cargar datos
with st.spinner("Cargando datos del mercado..."):
    stock_data = load_all_stock_data(tickers, data_type, start_date_str, end_date_str)

    if not stock_data:
        st.warning(f"No se encontraron datos para el rango de fechas especificado.")
        st.stop()

# Sección 1: Resumen del Mercado
st.header("Panorama General del Mercado")

# Crear métricas para el resumen
col1, col2, col3, col4 = st.columns(4)

# Calcular métricas generales
latest_data = {}
performance = {}
volatility = {}

for ticker, df in stock_data.items():
    if not df.empty:
        # Último precio
        latest_data[ticker] = df['close'].iloc[-1]

        # Rendimiento en el período
        first_price = df['close'].iloc[0]
        last_price = df['close'].iloc[-1]
        performance[ticker] = (last_price - first_price) / first_price * 100

        # Volatilidad
        volatility[ticker] = df['daily_return'].std() * np.sqrt(252) * 100

# Mostrar métricas
with col1:
    st.metric(
        "Acciones Analizadas",
        f"{len(stock_data)}",
        delta=None
    )

with col2:
    # Mejor rendimiento
    if performance:
        best_ticker = max(performance.items(), key=lambda x: x[1])
        st.metric(
            "Mejor Rendimiento",
            f"{best_ticker[0]}",
            f"{best_ticker[1]:.2f}%"
        )
    else:
        st.metric("Mejor Rendimiento", "N/A", "0.00%")

with col3:
    # Peor rendimiento
    if performance:
        worst_ticker = min(performance.items(), key=lambda x: x[1])
        st.metric(
            "Peor Rendimiento",
            f"{worst_ticker[0]}",
            f"{worst_ticker[1]:.2f}%"
        )
    else:
        st.metric("Peor Rendimiento", "N/A", "0.00%")

with col4:
    # Volatilidad promedio
    if volatility:
        avg_volatility = sum(volatility.values()) / len(volatility)
        st.metric(
            "Volatilidad Promedio",
            f"{avg_volatility:.2f}%",
            delta=None
        )
    else:
        st.metric("Volatilidad Promedio", "0.00%", delta=None)

# Sección 2: Mapa de Calor de Rendimientos
st.subheader("Mapa de Calor de Rendimientos")

# Crear DataFrame para el mapa de calor
returns_df = pd.DataFrame()

for ticker, df in stock_data.items():
    if not df.empty and 'daily_return' in df.columns:
        returns_df[ticker] = df['daily_return']

# Calcular matriz de correlación
if not returns_df.empty and returns_df.shape[1] > 1:
    corr_matrix = returns_df.corr()

    # Crear mapa de calor con Plotly
    fig = px.imshow(
        corr_matrix,
        text_auto=True,
        aspect="auto",
        color_continuous_scale='RdBu_r',
        zmin=-1, zmax=1
    )

    fig.update_layout(
        title="Correlación entre Acciones",
        height=600,
        width=800
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No hay datos suficientes para calcular la matriz de correlación.")

# Sección 3: Rendimiento Comparativo
st.subheader("Rendimiento Comparativo")

# Calcular rendimiento acumulado
cumulative_returns = pd.DataFrame()

for ticker, df in stock_data.items():
    if not df.empty and 'daily_return' in df.columns:
        cumulative_returns[ticker] = (1 + df['daily_return']).cumprod() - 1

# Crear gráfico con Plotly
if not cumulative_returns.empty:
    fig = go.Figure()

    for ticker in cumulative_returns.columns:
        fig.add_trace(go.Scatter(
            x=cumulative_returns.index,
            y=cumulative_returns[ticker] * 100,  # Convertir a porcentaje
            mode='lines',
            name=ticker
        ))

    fig.update_layout(
        xaxis_title="Fecha",
        yaxis_title="Rendimiento Acumulado (%)",
        legend_title="Acciones",
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No hay datos suficientes para calcular el rendimiento acumulado.")

# Sección 4: Distribución de Volumen
st.subheader("Distribución de Volumen de Negociación")

# Calcular volumen promedio para cada acción
volume_data = {}

for ticker, df in stock_data.items():
    if not df.empty and 'volume' in df.columns:
        volume_data[ticker] = df['volume'].mean()

# Crear gráfico de barras con Plotly
if volume_data:
    # Ordenar por volumen
    sorted_volume = dict(sorted(volume_data.items(), key=lambda x: x[1], reverse=True))

    fig = px.bar(
        x=list(sorted_volume.keys()),
        y=list(sorted_volume.values()),
        labels={'x': 'Acción', 'y': 'Volumen Promedio'},
        title="Volumen Promedio de Negociación",
        color=list(sorted_volume.values()),
        color_continuous_scale='Viridis'
    )

    fig.update_layout(height=500)

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No hay datos suficientes para mostrar la distribución de volumen.")

# Sección 5: Tabla de Resumen
st.subheader("Tabla de Resumen del Mercado")

# Crear DataFrame de resumen
summary_data = []

for ticker, df in stock_data.items():
    if not df.empty:
        last_row = df.iloc[-1]
        first_row = df.iloc[0]

        # Calcular métricas
        price_change = last_row['close'] - first_row['close']
        price_change_pct = price_change / first_row['close'] * 100

        # Volatilidad (desviación estándar anualizada de los retornos diarios)
        vol = df['daily_return'].std() * np.sqrt(252) * 100

        # Volumen promedio
        avg_volume = df['volume'].mean()

        # RSI actual
        current_rsi = last_row['rsi14']

        # Tendencia (basada en medias móviles)
        if 'ma20' in last_row and 'ma50' in last_row:
            if last_row['ma20'] > last_row['ma50']:
                trend = "Alcista"
            elif last_row['ma20'] < last_row['ma50']:
                trend = "Bajista"
            else:
                trend = "Neutral"
        else:
            trend = "N/A"

        # Añadir a la lista de resumen
        summary_data.append({
            'Ticker': ticker,
            'Último Precio': last_row['close'],
            'Cambio (€)': price_change,
            'Cambio (%)': price_change_pct,
            'Volatilidad (%)': vol,
            'Volumen Promedio': avg_volume,
            'RSI (14)': current_rsi,
            'Tendencia': trend
        })

# Crear DataFrame y mostrar
if summary_data:
    summary_df = pd.DataFrame(summary_data)

    # Ordenar por rendimiento
    summary_df = summary_df.sort_values(by='Cambio (%)', ascending=False)

    # Formatear columnas
    summary_df['Último Precio'] = summary_df['Último Precio'].map('€{:.2f}'.format)
    summary_df['Cambio (€)'] = summary_df['Cambio (€)'].map('{:+.2f}€'.format)
    summary_df['Cambio (%)'] = summary_df['Cambio (%)'].map('{:+.2f}%'.format)
    summary_df['Volatilidad (%)'] = summary_df['Volatilidad (%)'].map('{:.2f}%'.format)
    summary_df['Volumen Promedio'] = summary_df['Volumen Promedio'].map('{:,.0f}'.format)
    summary_df['RSI (14)'] = summary_df['RSI (14)'].map('{:.2f}'.format)

    # Aplicar estilo condicional
    def highlight_trend(val):
        if val == 'Alcista':
            return 'background-color: #d4f7d4'
        elif val == 'Bajista':
            return 'background-color: #f7d4d4'
        else:
            return ''

    # Mostrar tabla con estilo
    st.dataframe(
        summary_df.style.applymap(highlight_trend, subset=['Tendencia']),
        use_container_width=True
    )
else:
    st.warning("No hay datos suficientes para crear la tabla de resumen.")

# Sección 6: Distribución de Rendimientos
st.subheader("Distribución de Rendimientos")

# Crear histograma de rendimientos
if not returns_df.empty:
    # Seleccionar las 5 acciones con mayor volumen para no saturar el gráfico
    top_volume_tickers = sorted(volume_data.items(), key=lambda x: x[1], reverse=True)[:5]
    top_tickers = [ticker for ticker, _ in top_volume_tickers]

    fig = go.Figure()

    for ticker in top_tickers:
        if ticker in returns_df.columns:
            fig.add_trace(go.Histogram(
                x=returns_df[ticker] * 100,  # Convertir a porcentaje
                name=ticker,
                opacity=0.7,
                nbinsx=30
            ))

    fig.update_layout(
        title="Distribución de Rendimientos Diarios (Top 5 por Volumen)",
        xaxis_title="Rendimiento Diario (%)",
        yaxis_title="Frecuencia",
        barmode='overlay',
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No hay datos suficientes para mostrar la distribución de rendimientos.")

# Información adicional
st.markdown("---")
st.markdown("""
### Interpretación del Resumen del Mercado

Este dashboard proporciona una visión general del comportamiento de las principales acciones del IBEX 35:

- **Correlación**: Muestra qué acciones tienden a moverse juntas y cuáles se mueven en direcciones opuestas.
- **Rendimiento Comparativo**: Permite identificar qué acciones han tenido mejor desempeño en el período seleccionado.
- **Volumen de Negociación**: Indica la liquidez y el interés del mercado en cada acción.
- **RSI (Índice de Fuerza Relativa)**: Valores por encima de 70 sugieren sobrecompra, mientras que valores por debajo de 30 sugieren sobreventa.
- **Tendencia**: Basada en el cruce de medias móviles de 20 y 50 días.

Los datos se actualizan diariamente después del cierre del mercado español.
""")

# Cerrar conexión al finalizar
conn.close()