from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerTransformOperator,
)
from airflow.providers.amazon.aws.sensors.sagemaker_transform import (
    SageMakerTransformSensor,
)
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

default_args = {
    'aws_conn_id': 'aws_default',
    'region_name': 'us-east-1',
}

with DAG('batch_inference_dag',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=days_ago(1),
         catchup=False) as dag:

    # 1. Espera datos en S3
    wait_for_data = S3KeySensor(
        task_id='wait_for_input',
        bucket_key='data/input/*.csv',
        bucket_name='mi-bucket',
        aws_conn_id='aws_default',
        timeout=6*60*60,
        poke_interval=300,
    )

    # 2. Envia el trabajo de Batch Transform
    batch_transform = SageMakerTransformOperator(
        task_id='batch_transform',
        config={
            'TransformJobName': 'nlp-batch-{{ ts_nodash }}',
            'ModelName': 'nlp-pipeline-model-{{ git_sha }}',
            'TransformInput': {
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': 's3://mi-bucket/data/input/'
                    }
                },
                'ContentType': 'text/csv'
            },
            'TransformOutput': {
                'S3OutputPath': 's3://mi-bucket/data/output/'
            },
            'TransformResources': {
                'InstanceType': 'ml.m5.large',
                'InstanceCount': 2
            }
        }
    )

    # 3. Espera a que termine el job
    wait_for_batch = SageMakerTransformSensor(
        task_id='wait_for_batch_complete',
        transform_job_name='nlp-batch-{{ ts_nodash }}',
        aws_conn_id='aws_default',
        poke_interval=60,
        timeout=4*60*60,
    )

    # 4. Validación o procesamiento post-inferencia
    def process_outputs(**context):
        # aquí podrías mover/validar/registrar resultados
        pass

    process_results = PythonOperator(
        task_id='process_results',
        python_callable=process_outputs,
    )

    wait_for_data >> batch_transform >> wait_for_batch >> process_results
