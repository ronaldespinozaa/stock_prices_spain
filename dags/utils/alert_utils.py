import os
import logging
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_price_alerts(df, ticker, config):
    """
    Verifica si hay alertas de precio para una acción

    Args:
        df (pandas.DataFrame): DataFrame con datos de la acción
        ticker (str): Símbolo de la acción
        config (dict): Configuración del proyecto

    Returns:
        list: Lista de alertas generadas
    """
    alerts = []

    if df.empty or len(df) < 2:
        return alerts

    # Obtener últimos dos días de datos
    last_day = df.iloc[-1]
    prev_day = df.iloc[-2]

    # Verificar cambio porcentual significativo
    price_change_pct = (last_day['close'] - prev_day['close']) / prev_day['close'] * 100
    threshold = config['alerts']['conditions']['price_change_pct']

    if abs(price_change_pct) >= threshold:
        direction = "subido" if price_change_pct > 0 else "bajado"
        alert = {
            'ticker': ticker,
            'type': 'price_change',
            'message': f"{ticker} ha {direction} un {abs(price_change_pct):.2f}% en el último día",
            'severity': 'high' if abs(price_change_pct) >= threshold * 2 else 'medium',
            'date': last_day.name.strftime('%Y-%m-%d') if hasattr(last_day.name, 'strftime') else str(last_day.name)
        }
        alerts.append(alert)

    return alerts

def check_technical_alerts(df, ticker, config):
    """
    Verifica si hay alertas técnicas para una acción

    Args:
        df (pandas.DataFrame): DataFrame con datos de la acción
        ticker (str): Símbolo de la acción
        config (dict): Configuración del proyecto

    Returns:
        list: Lista de alertas generadas
    """
    alerts = []

    if df.empty or 'rsi14' not in df.columns:
        return alerts

    # Obtener último día de datos
    last_day = df.iloc[-1]

    # Verificar RSI en niveles de sobrecompra/sobreventa
    if 'rsi14' in last_day and pd.notna(last_day['rsi14']):
        rsi = last_day['rsi14']
        rsi_oversold = config['alerts']['conditions']['rsi_oversold']
        rsi_overbought = config['alerts']['conditions']['rsi_overbought']

        if rsi <= rsi_oversold:
            alert = {
                'ticker': ticker,
                'type': 'rsi_oversold',
                'message': f"{ticker} está en niveles de sobreventa (RSI: {rsi:.2f})",
                'severity': 'medium',
                'date': last_day.name.strftime('%Y-%m-%d') if hasattr(last_day.name, 'strftime') else str(last_day.name)
            }
            alerts.append(alert)

        elif rsi >= rsi_overbought:
            alert = {
                'ticker': ticker,
                'type': 'rsi_overbought',
                'message': f"{ticker} está en niveles de sobrecompra (RSI: {rsi:.2f})",
                'severity': 'medium',
                'date': last_day.name.strftime('%Y-%m-%d') if hasattr(last_day.name, 'strftime') else str(last_day.name)
            }
            alerts.append(alert)

    # Verificar cruce de medias móviles
    if 'ma20' in last_day and 'ma50' in last_day and pd.notna(last_day['ma20']) and pd.notna(last_day['ma50']):
        if len(df) >= 2:
            prev_day = df.iloc[-2]

            # Cruce alcista (MA20 cruza por encima de MA50)
            if prev_day['ma20'] <= prev_day['ma50'] and last_day['ma20'] > last_day['ma50']:
                alert = {
                    'ticker': ticker,
                    'type': 'ma_crossover_bullish',
                    'message': f"{ticker} ha generado un cruce alcista de medias móviles (MA20 > MA50)",
                    'severity': 'medium',
                    'date': last_day.name.strftime('%Y-%m-%d') if hasattr(last_day.name, 'strftime') else str(last_day.name)
                }
                alerts.append(alert)

            # Cruce bajista (MA20 cruza por debajo de MA50)
            elif prev_day['ma20'] >= prev_day['ma50'] and last_day['ma20'] < last_day['ma50']:
                alert = {
                    'ticker': ticker,
                    'type': 'ma_crossover_bearish',
                    'message': f"{ticker} ha generado un cruce bajista de medias móviles (MA20 < MA50)",
                    'severity': 'medium',
                    'date': last_day.name.strftime('%Y-%m-%d') if hasattr(last_day.name, 'strftime') else str(last_day.name)
                }
                alerts.append(alert)

    # Verificar MACD
    if 'macd' in last_day and 'macd_signal' in last_day and pd.notna(last_day['macd']) and pd.notna(last_day['macd_signal']):
        if len(df) >= 2:
            prev_day = df.iloc[-2]

            # Cruce alcista (MACD cruza por encima de la señal)
            if prev_day['macd'] <= prev_day['macd_signal'] and last_day['macd'] > last_day['macd_signal']:
                alert = {
                    'ticker': ticker,
                    'type': 'macd_crossover_bullish',
                    'message': f"{ticker} ha generado un cruce alcista de MACD",
                    'severity': 'medium',
                    'date': last_day.name.strftime('%Y-%m-%d') if hasattr(last_day.name, 'strftime') else str(last_day.name)
                }
                alerts.append(alert)

            # Cruce bajista (MACD cruza por debajo de la señal)
            elif prev_day['macd'] >= prev_day['macd_signal'] and last_day['macd'] < last_day['macd_signal']:
                alert = {
                    'ticker': ticker,
                    'type': 'macd_crossover_bearish',
                    'message': f"{ticker} ha generado un cruce bajista de MACD",
                    'severity': 'medium',
                    'date': last_day.name.strftime('%Y-%m-%d') if hasattr(last_day.name, 'strftime') else str(last_day.name)
                }
                alerts.append(alert)

    return alerts

def generate_alerts(conn, tickers, config):
    """
    Genera alertas para una lista de acciones

    Args:
        conn (sqlite3.Connection): Conexión a la base de datos
        tickers (list): Lista de símbolos de acciones
        config (dict): Configuración del proyecto

    Returns:
        list: Lista de alertas generadas
    """
    all_alerts = []

    try:
        # Obtener fecha actual
        today = datetime.now()

        # Obtener datos de los últimos 5 días para cada acción
        for ticker in tickers:
            # Calcular fecha de inicio (5 días atrás)
            start_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')

            # Consulta SQL para obtener datos recientes
            query = f'''
            SELECT *
            FROM daily_data
            WHERE ticker = ? AND date BETWEEN ? AND ?
            ORDER BY date
            '''

            # Ejecutar consulta
            df = pd.read_sql_query(query, conn, params=(ticker, start_date, end_date))

            # Convertir fecha a índice
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)

                # Verificar alertas de precio
                price_alerts = check_price_alerts(df, ticker, config)
                all_alerts.extend(price_alerts)

                # Verificar alertas técnicas
                technical_alerts = check_technical_alerts(df, ticker, config)
                all_alerts.extend(technical_alerts)

        return all_alerts

    except Exception as e:
        logger.error(f"Error al generar alertas: {str(e)}")
        return []

def send_email_alert(alerts, config):
    """
    Envía alertas por correo electrónico

    Args:
        alerts (list): Lista de alertas a enviar
        config (dict): Configuración del proyecto

    Returns:
        bool: True si se envió el correo correctamente
    """
    if not alerts or not config['alerts']['enabled']:
        return False

    try:
        # Configuración del servidor SMTP
        smtp_server = config['alerts']['email']['smtp_server']
        smtp_port = config['alerts']['email']['smtp_port']

        # Credenciales (deberían estar en variables de entorno)
        smtp_user = os.environ.get('SMTP_USER', '')
        smtp_password = os.environ.get('SMTP_PASSWORD', '')

        # Si no hay credenciales, no enviar correo
        if not smtp_user or not smtp_password:
            logger.warning("No se encontraron credenciales SMTP. No se enviará el correo.")
            return False

        # Destinatario (debería estar en variables de entorno o config)
        recipient = os.environ.get('ALERT_EMAIL', '')

        if not recipient:
            logger.warning("No se encontró destinatario para las alertas. No se enviará el correo.")
            return False

        # Crear mensaje
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = recipient
        msg['Subject'] = f"Alertas de Acciones - {datetime.now().strftime('%Y-%m-%d')}"

        # Crear cuerpo del mensaje
        body = "<html><body>"
        body += "<h2>Alertas de Acciones</h2>"
        body += f"<p>Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>"
        body += "<table border='1' style='border-collapse: collapse; width: 100%;'>"
        body += "<tr style='background-color: #f2f2f2;'><th>Acción</th><th>Tipo</th><th>Mensaje</th><th>Severidad</th><th>Fecha</th></tr>"

        for alert in alerts:
            # Color según severidad
            color = "#ff9999" if alert['severity'] == 'high' else "#ffffcc" if alert['severity'] == 'medium' else "#ccffcc"

            body += f"<tr style='background-color: {color};'>"
            body += f"<td>{alert['ticker']}</td>"
            body += f"<td>{alert['type']}</td>"
            body += f"<td>{alert['message']}</td>"
            body += f"<td>{alert['severity']}</td>"
            body += f"<td>{alert['date']}</td>"
            body += "</tr>"

        body += "</table>"
        body += "</body></html>"

        msg.attach(MIMEText(body, 'html'))

        # Enviar correo
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)

        logger.info(f"Correo de alertas enviado a {recipient}")
        return True

    except Exception as e:
        logger.error(f"Error al enviar correo de alertas: {str(e)}")
        return False

def process_alerts(conn, config, **kwargs):
    """
    Procesa y envía alertas

    Args:
        conn (sqlite3.Connection): Conexión a la base de datos
        config (dict): Configuración del proyecto

    Returns:
        bool: True si se procesaron las alertas correctamente
    """
    try:
        # Obtener lista de tickers
        tickers = config['stocks']['tickers']

        # Generar alertas
        alerts = generate_alerts(conn, tickers, config)

        if alerts:
            logger.info(f"Se generaron {len(alerts)} alertas")

            # Enviar alertas por correo
            if config['alerts']['enabled']:
                send_email_alert(alerts, config)

            # Aquí se podrían implementar otros canales de notificación
            # como Slack, Telegram, etc.
        else:
            logger.info("No se generaron alertas")

        return True

    except Exception as e:
        logger.error(f"Error al procesar alertas: {str(e)}")
        return False