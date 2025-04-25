import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import sqlite3
from datetime import datetime, timedelta
import yaml

# Añadir directorio raíz al path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.utils.db_utils import create_db_connection, get_stock_data, get_portfolio_metrics

# Cargar configuración
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'config.yaml')
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

# Configuración de la página
st.set_page_config(
    page_title=config['streamlit']['title'],
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# # Aplicar tema personalizado
# st.markdown(f"""
# <style>
#     .reportview-container .main .block-container{{
#         max-width: 1200px;
#         padding-top: 2rem;
#         padding-right: 2rem;
#         padding-left: 2rem;
#         padding-bottom: 2rem;
#     }}
#     .stApp {{
#         background-color: {config['streamlit']['theme']['background_color']};
#         color: {config['streamlit']['theme']['text_color']};
#     }}
#     .stButton>button {{
#         background-color: {config['streamlit']['theme']['primary_color']};
#         color: white;
#     }}
#     .stProgress > div > div > div > div {{
#         background-color: {config['streamlit']['theme']['primary_color']};
#     }}
#     .stSelectbox label, .stSlider label {{
#         color: {config['streamlit']['theme']['text_color']};
#     }}
# </style>
# """, unsafe_allow_html=True)

# Título de la aplicación
st.title("📈 Dashboard de Acciones Españolas")
st.markdown("Análisis de las principales acciones del IBEX 35")

# Función para obtener la conexión a la base de datos
@st.cache_resource
def get_connection():
    # Ruta a la base de datos
    data_path = os.environ.get('DATA_PATH', config['general']['data_path'])
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
st.sidebar.header("Filtros Generales")

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

# Selección de acciones
selected_tickers = st.sidebar.multiselect(
    "Seleccionar Acciones",
    options=tickers,
    default=tickers[:5]  # Seleccionar las primeras 5 por defecto
)

if not selected_tickers:
    st.warning("Por favor, selecciona al menos una acción.")
    st.stop()

# Función para cargar datos de acciones
@st.cache_data(ttl=3600)
def load_stock_data(ticker, data_type, start_date, end_date):
    return get_stock_data(conn, ticker, data_type, start_date, end_date)

# Cargar datos para las acciones seleccionadas
with st.spinner("Cargando datos..."):
    stock_data = {}
    for ticker in selected_tickers:
        df = load_stock_data(ticker, data_type, start_date_str, end_date_str)
        if not df.empty:
            stock_data[ticker] = df

# Sección principal
st.header("Resumen del Mercado")

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

# Gráfico de precios con Plotly
st.subheader("Evolución de Precios")

# Crear DataFrame para el gráfico
price_df = pd.DataFrame()

for ticker, df in stock_data.items():
    if not df.empty:
        price_df[ticker] = df['close']

# Mostrar gráfico
if not price_df.empty:
    fig = px.line(
        price_df,
        title="Evolución de Precios",
        labels={"value": "Precio (€)", "variable": "Acción", "date": "Fecha"},
        template="plotly_white"
    )

    # Personalizar gráfico
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=500,
        hovermode="x unified"
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No hay datos suficientes para mostrar el gráfico de precios.")

# Gráfico de rendimiento acumulado con Plotly
st.subheader("Rendimiento Acumulado")

# Crear DataFrame para el gráfico
returns_df = pd.DataFrame()

for ticker, df in stock_data.items():
    if not df.empty and 'daily_return' in df.columns:
        returns_df[ticker] = (1 + df['daily_return'].fillna(0)).cumprod() - 1

# Mostrar gráfico
if not returns_df.empty:
    fig = px.line(
        returns_df * 100,
        title="Rendimiento Acumulado (%)",
        labels={"value": "Rendimiento (%)", "variable": "Acción", "date": "Fecha"},
        template="plotly_white"
    )

    # Personalizar gráfico
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=500,
        hovermode="x unified"
    )

    # Añadir línea de referencia en 0%
    fig.add_shape(
        type="line",
        x0=returns_df.index[0],
        y0=0,
        x1=returns_df.index[-1],
        y1=0,
        line=dict(color="gray", width=1, dash="dash"),
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No hay datos suficientes para mostrar el gráfico de rendimiento acumulado.")

# Tabla de resumen
st.subheader("Tabla de Resumen")

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
        current_rsi = last_row['rsi14'] if 'rsi14' in last_row else None

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

    if 'RSI (14)' in summary_df.columns and summary_df['RSI (14)'].notna().any():
        summary_df['RSI (14)'] = summary_df['RSI (14)'].map(lambda x: '{:.2f}'.format(x) if pd.notna(x) else 'N/A')

    # Mostrar tabla
    st.dataframe(summary_df, use_container_width=True)
else:
    st.warning("No hay datos suficientes para crear la tabla de resumen.")

# Gráfico de correlación
if len(selected_tickers) > 1:
    st.subheader("Matriz de Correlación")

    # Calcular matriz de correlación
    corr_df = pd.DataFrame()

    for ticker, df in stock_data.items():
        if not df.empty and 'daily_return' in df.columns:
            corr_df[ticker] = df['daily_return']

    if not corr_df.empty:
        corr_matrix = corr_df.corr()

        # Crear gráfico de correlación
        fig = px.imshow(
            corr_matrix,
            text_auto=True,
            color_continuous_scale='RdBu_r',
            title="Correlación entre Acciones",
            labels=dict(color="Correlación"),
            zmin=-1, zmax=1
        )

        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No hay datos suficientes para mostrar la matriz de correlación.")

# Gráfico de velas (Candlestick)
if len(selected_tickers) == 1:
    st.subheader(f"Gráfico de Velas - {selected_tickers[0]}")

    # Obtener datos para el gráfico de velas
    ticker = selected_tickers[0]
    df = stock_data[ticker]

    if not df.empty:
        # Crear gráfico de velas
        fig = go.Figure(data=[go.Candlestick(
            x=df.index,
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name=ticker
        )])

        # Añadir medias móviles si están disponibles
        if 'ma20' in df.columns:
            fig.add_trace(go.Scatter(
                x=df.index,
                y=df['ma20'],
                line=dict(color='blue', width=1),
                name='MA 20'
            ))

        if 'ma50' in df.columns:
            fig.add_trace(go.Scatter(
                x=df.index,
                y=df['ma50'],
                line=dict(color='orange', width=1),
                name='MA 50'
            ))

        # Personalizar gráfico
        fig.update_layout(
            title=f"{ticker} - Gráfico de Velas",
            xaxis_title="Fecha",
            yaxis_title="Precio (€)",
            height=600,
            template="plotly_white",
            xaxis_rangeslider_visible=False
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(f"No hay datos suficientes para mostrar el gráfico de velas de {ticker}.")

# Información adicional
st.markdown("---")
st.markdown("""
### Información del Dashboard

Este dashboard proporciona una visión general del comportamiento de las principales acciones del IBEX 35.

Utiliza los filtros en la barra lateral para:
- Seleccionar el rango de fechas a analizar
- Elegir entre datos históricos o diarios
- Seleccionar las acciones específicas a visualizar

Para análisis más detallados, visita las diferentes páginas disponibles en el menú lateral.

Los datos se actualizan diariamente después del cierre del mercado español.
""")

# Cerrar conexión al finalizar
conn.close()