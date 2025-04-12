# api/app.py

from flask import Flask, jsonify, request
import sqlite3
import os
import sys
import pandas as pd
from datetime import datetime, timedelta

# Añadir directorio raíz al path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.utils.db_utils import create_db_connection, get_stock_data

app = Flask(__name__)

# Ruta a la base de datos
data_path = os.environ.get('DATA_PATH', os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data'))
db_path = os.path.join(data_path, 'database', 'stocks.db')

@app.route('/api/tickers', methods=['GET'])
def get_tickers():
    """
    Devuelve la lista de tickers disponibles
    """
    conn = create_db_connection(db_path)
    if conn is None:
        return jsonify({"error": "No se pudo conectar a la base de datos"}), 500

    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT ticker FROM historical_data")
    tickers = [row[0] for row in cursor.fetchall()]
    conn.close()

    return jsonify({"tickers": tickers})

@app.route('/api/stock/<ticker>', methods=['GET'])
def get_stock(ticker):
    """
    Devuelve los datos de una acción específica
    """
    # Parámetros de consulta
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    data_type = request.args.get('type', 'daily')

    conn = create_db_connection(db_path)
    if conn is None:
        return jsonify({"error": "No se pudo conectar a la base de datos"}), 500

    df = get_stock_data(conn, ticker, data_type, start_date, end_date)
    conn.close()

    if df.empty:
        return jsonify({"error": f"No se encontraron datos para {ticker}"}), 404

    # Convertir DataFrame a diccionario
    df['date'] = df.index.strftime('%Y-%m-%d')
    result = df.to_dict(orient='records')

    return jsonify({"ticker": ticker, "data": result})

@app.route('/api/summary', methods=['GET'])
def get_summary():
    """
    Devuelve un resumen del mercado
    """
    conn = create_db_connection(db_path)
    if conn is None:
        return jsonify({"error": "No se pudo conectar a la base de datos"}), 500

    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT ticker FROM historical_data")
    tickers = [row[0] for row in cursor.fetchall()]

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    summary = []

    for ticker in tickers:
        df = get_stock_data(conn, ticker, 'daily', start_date, end_date)

        if not df.empty:
            last_row = df.iloc[-1]
            first_row = df.iloc[0]

            # Calcular métricas
            price_change = last_row['close'] - first_row['close']
            price_change_pct = price_change / first_row['close'] * 100

            summary.append({
                "ticker": ticker,
                "last_price": float(last_row['close']),
                "change": float(price_change),
                "change_pct": float(price_change_pct),
                "volume": float(last_row['volume']),
                "date": last_row.name.strftime('%Y-%m-%d')
            })

    conn.close()

    return jsonify({"summary": summary})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)