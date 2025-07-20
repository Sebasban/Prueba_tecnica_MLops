import os
import pickle
import joblib
import pandas as pd
import glob
import logging

# Configuración básica de logging
t=logging.basicConfig(level=logging.INFO,
                          format="%(asctime)s %(levelname)s %(message)s")

# Directorios estándar de SageMaker Batch Transform
MODEL_DIR = '/opt/ml/model'
INPUT_DIR = '/opt/ml/input/data'
OUTPUT_DIR = '/opt/ml/output'


def load_artifacts(model_dir: str):
    # Ruta al vectorizador
    vect_file = os.path.join(model_dir, 'vectorizer_tfidf.pkl')
    if not os.path.exists(vect_file):
        raise FileNotFoundError(f"Vectorizer no encontrado en {vect_file}")
    logging.info(f"Cargando vectorizador desde {vect_file}")
    vectorizer = joblib.load(vect_file)

    # Encuentra el modelo .pkl (excluyendo el vectorizador)
    model_file = None
    for fname in os.listdir(model_dir):
        if fname.endswith('.pkl') and fname != 'vectorizer_tfidf.pkl':
            model_file = os.path.join(model_dir, fname)
            break
    if model_file is None:
        raise FileNotFoundError(f"Archivo de modelo .pkl no encontrado en {model_dir}")
    logging.info(f"Cargando modelo desde {model_file}")
    with open(model_file, 'rb') as f:
        model = pickle.load(f)

    return vectorizer, model


def run_batch_inference():
    vectorizer, model = load_artifacts(MODEL_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Listado de archivos de entrada
    input_files = glob.glob(os.path.join(INPUT_DIR, '*.csv'))
    if not input_files:
        logging.warning(f"No se encontraron archivos CSV en {INPUT_DIR}")
        return

    for filepath in input_files:
        logging.info(f"Procesando archivo {filepath}")
        df = pd.read_csv(filepath)
        if 'text_clean' not in df.columns:
            logging.error("Columna 'text_clean' ausente en los datos de entrada")
            continue

        # Vectorización de texto
        X = vectorizer.transform(df['text_clean'])
        preds = model.predict(X)
        result_df = df.copy()
        result_df['prediction'] = preds

        # Guarda resultados
        out_name = os.path.basename(filepath).replace('.csv', '_predictions.csv')
        out_path = os.path.join(OUTPUT_DIR, out_name)
        logging.info(f"Guardando resultados en {out_path}")
        result_df.to_csv(out_path, index=False)


if __name__ == '__main__':
    run_batch_inference()
