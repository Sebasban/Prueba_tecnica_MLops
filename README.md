Prueba Técnica MLops
Este repositorio incluye dos componentes principales para la prueba técnica de MLOps en análisis de riesgos:
PruebaTecnicaMLopsRiesgos: Pipeline de ETL y entrenamiento de un modelo de clasificación de riesgos.
dags/: Definición de DAGs de Apache Airflow para la orquestación de tareas de procesamiento de datos y transformación con SageMaker.
PruebaTecnicaMLopsRiesgos/: Código fuente Python para extracción, transformación y carga (ETL), preprocesamiento de texto y entrenamiento de modelos.
notebooks/: Notebooks de Jupyter para la exploración de datos y desarrollo de modelos (clasificador y clustering).
tests/: Pruebas unitarias con pytest para asegurar la calidad del código.
PruebaTecnicaMLopsRiesgosInference: Script de inferencia (inference.py) para realizar predicciones con el modelo entrenado.
En la raíz del proyecto también encontrarás:
DOCKERFILE: Define la imagen Docker para ejecutar el script de inferencia.
requirements.txt: Dependencias para entrenamiento y modelado.
requirements_etl.txt: Dependencias para el pipeline ETL (Airflow, Spark y proveedores de AWS).
.github/workflows/: Configuración de GitHub Actions para CI CD (lint y pruebas).
reports/: Reportes de test en formato JUnit XML.

Requisitos
Python 3.10 o superior
Docker (opcional, para inferencia en contenedor)
AWS CLI configurado (para conectar con S3 y SageMaker)
Apache Airflow (para ejecutar los DAGs de ETL)
Instalación
Clonar el repositorio:
git clone git@github.com:Sebasban/Prueba_tecnica_MLops.git
cd Prueba_tecnica_MLops

Instalar dependencias para entrenamiento y modelado:

pip install --upgrade pip
pip install -r requirements.txt

Instalar dependencias para el pipeline ETL:

pip install -r requirements_etl.txt

Ejecución local
Pruebas: Ejecuta pytest en la raíz del proyecto.
Notebooks: Abre los notebooks en PruebaTecnicaMLopsRiesgos/notebooks/.
Airflow: Inicia Airflow y ejecuta los DAGs definidos en PruebaTecnicaMLopsRiesgos/dags/NLP.py.
Inferencia:
Local:
python PruebaTecnicaMLopsRiesgosInference/inference.py

Estructura de directorios

Prueba_tecnica_MLops/
├── PruebaTecnicaMLopsRiesgos/
│   ├── dags/
│   ├── PruebaTecnicaMLopsRiesgos/
│   ├── notebooks/
│   └── tests/
├── PruebaTecnicaMLopsRiesgosInference/
│   └── inference.py
├── requirements.txt
├── requirements_etl.txt
├── DOCKERFILE
├── .github/
└── reports/

Contribuciones
Sebastian Buritica Montoya