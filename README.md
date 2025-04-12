# Guía Detallada para Ejecutar el Proyecto ETL de Acciones Españolas

## Índice
1. [Requisitos Previos](#requisitos-previos)
2. [Estructura del Proyecto](#estructura-del-proyecto)
3. [Instalación](#instalación)
4. [Configuración](#configuración)
5. [Ejecución con Docker](#ejecución-con-docker)
6. [Ejecución Manual](#ejecución-manual)
7. [Acceso a las Interfaces](#acceso-a-las-interfaces)
8. [Solución de Problemas](#solución-de-problemas)
9. [Personalización](#personalización)
10. [Preguntas Frecuentes](#preguntas-frecuentes)

## Requisitos Previos

Para ejecutar este proyecto necesitarás:

- **Docker** y **Docker Compose** (recomendado)
- Alternativamente, Python 3.9+ con pip
- Conocimientos básicos de línea de comandos
- Conexión a Internet (para descargar datos de acciones)

## Estructura del Proyecto

El proyecto tiene la siguiente estructura de carpetas:

```
etl_acciones_espana/
├── dags/                  # DAGs de Airflow
│   ├── historical_etl.py  # DAG para datos históricos
│   ├── daily_etl.py       # DAG para actualizaciones diarias
│   └── utils/             # Utilidades para los DAGs
│       ├── db_utils.py    # Funciones para base de datos
│       ├── stock_utils.py # Funciones para datos de acciones
│       └── alert_utils.py # Funciones para alertas
├── data/                  # Datos generados
│   ├── database/          # Base de datos SQLite
│   └── temp/              # Archivos temporales
├── streamlit/             # Aplicación Streamlit
│   ├── app.py             # Aplicación principal
│   └── pages/             # Páginas adicionales
│       └── 01_Resumen_Mercado.py
├── docker/                # Archivos Docker
│   ├── Dockerfile.airflow # Dockerfile para Airflow
│   └── Dockerfile.streamlit # Dockerfile para Streamlit
├── config/                # Configuración
│   └── config.yaml        # Archivo de configuración
├── docker-compose.yml     # Configuración de Docker Compose
├── requirements.txt       # Dependencias de Python
└── README.md              # Documentación
```

## Instalación

### Opción 1: Usando Docker (Recomendado)

1. Clona el repositorio:
   ```bash
   git clone https://github.com/tu-usuario/etl_acciones_espana.git
   cd etl_acciones_espana
   ```

2. Construye y ejecuta los contenedores:
   ```bash
   docker-compose up --build
   ```

### Opción 2: Instalación Manual

1. Clona el repositorio:
   ```bash
   git clone https://github.com/tu-usuario/etl_acciones_espana.git
   cd etl_acciones_espana
   ```

2. Crea un entorno virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate  # En Windows: venv\Scripts\activate
   ```

3. Instala las dependencias:
   ```bash
   pip install -r requirements.txt
   ```

## Configuración

### Configuración del Proyecto

El archivo `config/config.yaml` contiene la configuración principal del proyecto:

- **Acciones a analizar**: Modifica la sección `stocks.tickers` para cambiar las acciones.
- **Indicadores técnicos**: Ajusta los parámetros en la sección `indicators`.
- **Alertas**: Configura las condiciones de alerta en `alerts.conditions`.

### Variables de Entorno

Para las alertas por correo electrónico, configura estas variables de entorno:

```bash
export SMTP_USER=tu_correo@gmail.com
export SMTP_PASSWORD=tu_contraseña
export ALERT_EMAIL=destinatario@ejemplo.com
```

En Windows:
```cmd
set SMTP_USER=tu_correo@gmail.com
set SMTP_PASSWORD=tu_contraseña
set ALERT_EMAIL=destinatario@ejemplo.com
```

## Ejecución con Docker

### Iniciar todos los servicios

```bash
docker-compose up
```

Para ejecutar en segundo plano:
```bash
docker-compose up -d
```

### Detener los servicios

```bash
docker-compose down
```

### Ver logs

```bash
docker-compose logs -f
```

Para un servicio específico:
```bash
docker-compose logs -f airflow-webserver
```

## Ejecución Manual

### Inicializar Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### Iniciar Airflow

En una terminal:
```bash
airflow webserver -p 8080
```

En otra terminal:
```bash
airflow scheduler
```

### Iniciar Streamlit

```bash
cd streamlit
streamlit run app.py
```

## Acceso a las Interfaces

### Airflow

- **URL**: [http://localhost:8080](http://localhost:8080)
- **Usuario**: admin
- **Contraseña**: admin

### Streamlit

- **URL**: [http://localhost:8501](http://localhost:8501)

## Solución de Problemas

### Base de datos no encontrada

Si Streamlit muestra "Base de datos no encontrada", asegúrate de que:

1. Los DAGs de Airflow se han ejecutado correctamente
2. La ruta a la base de datos es correcta en `config.yaml`
3. Los volúmenes de Docker están configurados correctamente

### Error al obtener datos de acciones

Si yfinance no puede obtener datos:

1. Verifica tu conexión a Internet
2. Comprueba que los símbolos de las acciones son correctos (deben terminar en `.MC` para acciones españolas)
3. Asegúrate de que el mercado ha estado abierto en las fechas solicitadas

### Problemas con Docker

Si tienes problemas con Docker:

```bash
# Detener todos los contenedores
docker-compose down

# Eliminar volúmenes
docker-compose down -v

# Reconstruir imágenes
docker-compose build --no-cache

# Iniciar de nuevo
docker-compose up
```

## Personalización

### Añadir nuevas acciones

1. Edita `config/config.yaml`
2. Añade nuevos símbolos a la lista `stocks.tickers`
3. Reinicia los servicios

### Crear nuevas visualizaciones

1. Crea un nuevo archivo en `streamlit/pages/`, por ejemplo `02_Analisis_Tecnico.py`
2. Implementa la visualización usando Streamlit
3. La nueva página aparecerá automáticamente en el menú lateral

### Modificar indicadores técnicos

1. Edita `config/config.yaml`
2. Ajusta los parámetros en la sección `indicators`
3. Reinicia los servicios y ejecuta los DAGs nuevamente

## Preguntas Frecuentes

### ¿Con qué frecuencia se actualizan los datos?

El DAG de actualización diaria está programado para ejecutarse a las 20:00 en días laborables (lunes a viernes), después del cierre del mercado español.

### ¿Puedo analizar acciones de otros mercados?

Sí, puedes modificar los símbolos en `config.yaml`. Para acciones de otros mercados, usa los sufijos adecuados:
- EE.UU.: sin sufijo (ej. `AAPL`)
- Reino Unido: `.L` (ej. `BP.L`)
- Alemania: `.DE` (ej. `BMW.DE`)

### ¿Cómo puedo añadir más indicadores técnicos?

1. Modifica `dags/utils/stock_utils.py` para implementar el nuevo indicador
2. Actualiza las tablas de la base de datos en `dags/utils/db_utils.py`
3. Añade el nuevo indicador a la configuración en `config.yaml`

### ¿Cómo puedo personalizar las alertas?

Edita `config/config.yaml` y ajusta los umbrales en la sección `alerts.conditions`. También puedes modificar `dags/utils/alert_utils.py` para implementar nuevos tipos de alertas o canales de notificación.

---

## Notas Adicionales

- Los datos históricos se cargan una sola vez al ejecutar el DAG `historical_data_etl`
- Las actualizaciones diarias se realizan con el DAG `daily_update_etl`
- La aplicación Streamlit proporciona visualizaciones interactivas de los datos
- Las alertas se envían por correo electrónico cuando se cumplen ciertas condiciones

Para más información, consulta la documentación oficial de [Apache Airflow](https://airflow.apache.org/docs/), [Streamlit](https://docs.streamlit.io/) y [yfinance](https://pypi.org/project/yfinance/).