# ETL Completo de Datos de Acciones con Base de Datos

He implementado un flujo ETL completo que incluye:
1. **Extracción** (E): Descarga de datos históricos con manejo robusto de rate limiting
2. **Transformación** (T): Procesamiento de datos crudos y cálculo de indicadores técnicos
3. **Carga** (L): Almacenamiento en la base de datos SQLite configurada en tu `config.yaml`

## Estructura de Archivos

La solución completa incluye los siguientes archivos:

```
stock_prices_spain/
├── src/
│   ├── config/
│   │   ├── __init__.py
│   │   └── config.py                # Lee la configuración existente
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── retry_utils.py           # Mecanismo de reintentos
│   │   └── stock_utils.py           # Funciones para datos de acciones
│   └── etl/
│       ├── __init__.py
│       ├── extraction/
│       │   ├── __init__.py
│       │   └── historical_extraction.py  # Extractor de datos
│       ├── processing/
│       │   ├── __init__.py
│       │   └── data_processor.py    # Procesador de datos
│       └── loading/
│           ├── __init__.py
│           └── db_loader.py         # Cargador de base de datos
├── scripts/
│   ├── download_historical_data.py  # Solo extracción
│   └── run_complete_etl.py          # ETL completo
└── config.yaml                      # Tu archivo de configuración existente
```

## Funcionalidades Principales

### 1. Extracción (historical_extraction.py)
- Descarga datos de Yahoo Finance con manejo robusto de rate limiting
- Procesa símbolos en lotes para evitar "Too Many Requests"
- Implementa backoff exponencial con jitter aleatorio para reintentos
- Genera informes detallados de la extracción
- Mantiene el formato CSV específico de tu proyecto

### 2. Procesamiento (data_processor.py)
- Lee los archivos CSV crudos respetando tu formato específico
- Calcula indicadores técnicos como SMA, EMA, rendimientos diarios y volatilidad
- Prepara los datos para su carga en la base de datos
- Guarda los datos procesados en un directorio separado

### 3. Carga en Base de Datos (db_loader.py)
- Inicializa la estructura de la base de datos SQLite (si no existe)
- Carga datos procesados en la tabla `stocks`
- Maneja actualizaciones de datos existentes
- Gestiona errores y transacciones

### 4. Script ETL Completo (run_complete_etl.py)
- Ejecuta el flujo ETL completo en un solo comando
- Permite ejecutar fases específicas (extracción, procesamiento, carga)
- Genera logs detallados de todo el proceso
- Integrado con tu configuración existente

## Estructura de la Base de Datos

Se crea una tabla `stocks` con la siguiente estructura:

```sql
CREATE TABLE IF NOT EXISTS stocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    price REAL,
    volume INTEGER,
    daily_return REAL,
    sma_20 REAL,
    ema_20 REAL,
    sma_50 REAL,
    volatility_20d REAL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, date)
)
```

También se crean índices para optimizar consultas comunes:
- `idx_stocks_ticker` para búsquedas por ticker
- `idx_stocks_date` para búsquedas por fecha

## Cómo Implementar

1. **Crear la estructura de directorios**:
   ```bash
   mkdir -p src/config src/utils src/etl/extraction src/etl/processing src/etl/loading scripts
   touch src/config/__init__.py src/utils/__init__.py src/etl/__init__.py src/etl/extraction/__init__.py src/etl/processing/__init__.py src/etl/loading/__init__.py
   ```

2. **Copiar los archivos proporcionados**:
   - Todos los archivos Python en sus respectivos directorios

3. **Instalar dependencias**:
   ```bash
   pip install yfinance pandas pyyaml
   ```

4. **Ejecutar el ETL completo**:
   ```bash
   python scripts/run_complete_etl.py
   ```

## Uso del Script ETL Completo

### Ejecutar el flujo completo:

```bash
python scripts/run_complete_etl.py
```

### Ejecutar solo fases específicas:

```bash
# Solo extracción y procesamiento (omitir carga en DB)
python scripts/run_complete_etl.py --skip-loading

# Solo procesamiento y carga (omitir extracción)
python scripts/run_complete_etl.py --skip-extraction

# Solo carga en DB de datos ya procesados
python scripts/run_complete_etl.py --skip-extraction --skip-processing
```

### Configurar la extracción:

```bash
# Descargar datos históricos más extensos
python scripts/run_complete_etl.py --period 5y

# Configuración optimizada para evitar rate limiting
python scripts/run_complete_etl.py --batch-size 3 --max-workers 2 --delay 15
```

## Estructura de Directorios

La solución utiliza los siguientes directorios basados en tu `general.data_path`:

- **{data_path}/raw**: Datos históricos crudos descargados de Yahoo Finance
- **{data_path}/processed**: Datos procesados con indicadores técnicos
- **{data_path}/reports**: Informes de ejecución del ETL
- **{data_path}/logs**: Archivos de logs detallados
- **{database.path}**: Base de datos SQLite (según tu configuración)

## Indicadores Técnicos Calculados

Los datos procesados incluyen:

1. **SMA_20**: Media móvil simple de 20 días
2. **SMA_50**: Media móvil simple de 50 días
3. **EMA_20**: Media móvil exponencial de 20 días
4. **Daily_Return**: Rendimiento diario (porcentaje)
5. **Volatility_20d**: Volatilidad (desviación estándar de rendimientos de 20 días)

## Integración con Airflow

Si deseas seguir usando Airflow pero con esta implementación más robusta:

```python
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ibex35_etl_complete',
    default_args=default_args,
    description='ETL completo de datos de acciones IBEX 35',
    schedule_interval='@daily',
    catchup=False
)

etl_task = BashOperator(
    task_id='run_complete_etl',
    bash_command='cd /ruta/a/tu/proyecto && python scripts/run_complete_etl.py',
    dag=dag
)
```

## Solución de Problemas

### Error de rate limiting

Si sigues experimentando errores de "Too Many Requests", intenta:

```bash
python scripts/run_complete_etl.py --batch-size 2 --max-workers 1 --delay 20
```

Esta configuración es extremadamente conservadora y debería resolver cualquier problema de rate limiting.

### Errores de acceso a la base de datos

Verifica:
1. Que el directorio padre de tu `database.path` exista
2. Que tengas permisos de escritura en la ubicación de la base de datos
3. Que no haya otra aplicación bloqueando el archivo de la base de datos

### Datos faltantes o vacíos

Si algún símbolo no tiene datos:
1. Verifica los archivos de log en `{data_path}/logs`
2. Revisa el informe de extracción en `{data_path}/reports`
3. Prueba a descargar ese símbolo específico:
   ```bash
   python scripts/run_complete_etl.py --symbols SAN.MC
   ```