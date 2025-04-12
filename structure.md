import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import zipfile

# Crear estructura de directorios para el repositorio
repo_structure = {
    "etl_acciones_espana": {
        "dags": {
            "historical_data_dag.py": None,
            "daily_update_dag.py": None,
            "utils": {
                "__init__.py": None,
                "stock_utils.py": None,
                "db_utils.py": None,
            }
        },
        "data": {
            "raw": {},
            "processed": {},
            "database": {
                ".gitkeep": None
            }
        },
        "logs": {
            ".gitkeep": None
        },
        "streamlit": {
            "app.py": None,
            "pages": {
                "01_Resumen_Mercado.py": None,
                "02_Analisis_Tecnico.py": None,
                "03_Comparativa.py": None,
            },
            "utils": {
                "__init__.py": None,
                "visualization.py": None,
            }
        },
        "tests": {
            "__init__.py": None,
            "test_stock_utils.py": None,
            "test_db_utils.py": None,
        },
        "docker": {
            "Dockerfile": None,
            "docker-compose.yml": None,
        },
        ".env.example": None,
        ".gitignore": None,
        "README.md": None,
        "requirements.txt": None,
        "setup.py": None,
    }
}

# Función para crear la estructura de directorios
def create_directory_structure(structure, base_path=""):
    for name, contents in structure.items():
        path = os.path.join(base_path, name)
        if contents is None:
            # Es un archivo, crear un archivo vacío
            with open(path, 'w') as f:
                pass
        else:
            # Es un directorio, crearlo y procesar su contenido
            os.makedirs(path, exist_ok=True)
            create_directory_structure(contents, path)

# Crear estructura temporal para generar los archivos
temp_dir = "etl_acciones_espana"
os.makedirs(temp_dir, exist_ok=True)
create_directory_structure(repo_structure, temp_dir)

# Contenido para los archivos principales

# README.md
readme_content = """# ETL de Acciones del Mercado Español

Este repositorio contiene un proyecto ETL (Extract, Transform, Load) para obtener, procesar y analizar datos de las 10 principales acciones del mercado español (IBEX 35) utilizando Apache Airflow y yfinance.

## Estructura del Proyecto
etl_acciones_espana/
├── dags/ # DAGs de Airflow
│ ├── historical_data_dag.py # DAG para extraer datos históricos (2 años)
│ ├── daily_update_dag.py # DAG para actualizaciones diarias
│ └── utils/ # Utilidades compartidas para los DAGs
├── data/ # Directorio para almacenar datos
│ ├── raw/ # Datos sin procesar
│ ├── processed/ # Datos procesados
│ └── database/ # Archivos de base de datos SQLite
├── logs/ # Logs de la aplicación
├── streamlit/ # Aplicación Streamlit para visualización
│ ├── app.py # Punto de entrada de la aplicación
│ └── pages/ # Páginas adicionales de la aplicación
├── tests/ # Tests unitarios y de integración
├── docker/ # Archivos para Docker
└── requirements.txt # Dependencias del proyecto


## Acciones Analizadas

El proyecto se centra en las siguientes 10 acciones principales del IBEX 35:

1. Santander (SAN.MC)
2. BBVA (BBVA.MC)
3. Iberdrola (IBE.MC)
4. Inditex (ITX.MC)
5. Telefónica (TEF.MC)
6. Repsol (REP.MC)
7. Caixabank (CABK.MC)
8. Naturgy (NTGY.MC)
9. Ferrovial (FER.MC)
10. Amadeus (AMS.MC)

## Funcionalidades

- **Extracción de datos históricos**: DAG que extrae datos de los últimos 2 años para las 10 acciones.
- **Actualizaciones diarias**: DAG que actualiza diariamente los datos de las acciones.
- **Almacenamiento en base de datos**: Los datos se almacenan en SQLite para facilitar el acceso.
- **Visualización con Streamlit**: Interfaz web para analizar y visualizar los datos.
truc
## Requisitos

- Python 3.8+
- Apache Airflow 2.5+
- yfinance
- pandas
- SQLite
- Streamlit

## Instalación

1. Clonar el repositorio:
```bash
git clone https://github.com/tu-usuario/etl_acciones_espana.git
cd etl_acciones_espana
2, Crear y activar un entorno virtual:
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\\Scripts\\activate 
```bash

Instalar dependencias:
bash
Copy Code
pip install -r requirements.txt
Configurar variables de entorno:
bash
Copy Code
cp .env.example .env
# Editar .env con tus configuraciones
Inicializar la base de datos de Airflow:
bash
Copy Code
airflow db init
Crear un usuario para la interfaz web de Airflow:
bash
Copy Code
airflow users create \\
    --username admin \\
    --firstname Admin \\
    --lastname User \\
    --role Admin \\
    --email admin@example.com
    

    Uso
Iniciar Airflow
bash
Copy Code
# Terminal 1: Iniciar el webserver
airflow webserver --port 8080

# Terminal 2: Iniciar el scheduler
airflow scheduler
Accede a la interfaz web de Airflow en http://localhost:8080

Ejecutar DAGs
Extracción de datos históricos:
Activa el DAG ibex35_historical_data desde la interfaz web de Airflow.
Este DAG extraerá datos de los últimos 2 años para las 10 acciones.
Actualizaciones diarias:
El DAG ibex35_daily_update se ejecutará automáticamente cada día a las 18:00 (después del cierre del mercado).
También puedes activarlo manualmente desde la interfaz web.
Visualización con Streamlit
bash
Copy Code
cd streamlit
streamlit run app.py
Accede a la aplicación Streamlit en http://localhost:8501

Desarrollo con Docker
Para ejecutar el proyecto en contenedores Docker:

bash
Copy Code
cd docker
docker-compose up -d
Contribuir
Haz un fork del repositorio
Crea una rama para tu funcionalidad (git checkout -b feature/nueva-funcionalidad)
Haz commit de tus cambios (git commit -am 'Añadir nueva funcionalidad')
Haz push a la rama (git push origin feature/nueva-funcionalidad)
Crea un Pull Request
Licencia
Este proyecto está licenciado bajo la Licencia MIT - ver el archivo LICENSE para más detalles.
"""