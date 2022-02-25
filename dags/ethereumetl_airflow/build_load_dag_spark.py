import logging
import os
from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.sensors.s3_key_sensor import S3KeySensor

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_load_dag_spark(
        dag_id,
        output_bucket,
        chain='ethereum',
        notification_emails=None,
        spark_conf=None,
        load_start_date=datetime(2018, 7, 1),
        schedule_interval='0 0 * * *',
        load_all_partitions=True
):
    # The following datasets must be created in Spark:
    # - crypto_{chain}_raw
    # - crypto_{chain}_temp
    # - crypto_{chain}

    dataset_name = f'{chain}'
    dataset_name_raw = f'{chain}_raw'
    dataset_name_temp = f'{chain}_temp'

    if not spark_conf:
        raise ValueError('k8s_config is required')

    environment = {
        'dataset_name': dataset_name,
        'dataset_name_raw': dataset_name_raw,
        'dataset_name_temp': dataset_name_temp,
        'load_all_partitions': load_all_partitions
    }

    def read_file(filepath):
        with open(filepath) as file_handle:
            content = file_handle.read()
            return content

    def render_template(template, context):
        template_replaced = template
        for key, value in context.items():
            template_replaced = template_replaced.replace('{{params.' + key + '}}', value)
        return template_replaced

    default_dag_args = {
        'depends_on_past': False,
        'start_date': load_start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    # Define a DAG (directed acyclic graph) of tasks.
    dag = models.DAG(
        dag_id,
        catchup=False,
        schedule_interval=schedule_interval,
        default_args=default_dag_args
    )

    dags_folder = os.environ.get('DAGS_FOLDER', '/opt/airflow/dags/repo/dags')
    temp_folder = '/tmp'

    def add_load_tasks(task, file_format):
        bucket_file_key = 'export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
            task=task, datestamp='{{ds}}', file_format=file_format
        )

        wait_sensor = S3KeySensor(
            task_id='wait_latest_{task}'.format(task=task),
            timeout=60 * 60,
            poke_interval=60,
            bucket_key=bucket_file_key,
            bucket_name=output_bucket,
            dag=dag
        )

        sql_path = os.path.join(dags_folder, 'resources/stages/raw/sqls_spark/{task}.sql'.format(task=task))
        sql_template = read_file(sql_path)
        sql = render_template(sql_template, {
            'database': dataset_name_temp,
            'file_path': 's3a://{bucket}/{bucket_name}'.format(bucket=output_bucket, bucket_name=bucket_file_key)
        })

        pyspark_path = os.path.join(temp_folder, '{task}.py'.format(task=task))
        pyspark_template_path = os.path.join(dags_folder, 'resources/stages/raw/sqls_spark/load_table.py.template')
        pyspark_template = read_file(pyspark_template_path)
        pyspark = render_template(pyspark_template, {
            'sql': sql,
            'database': dataset_name_temp,
            'table': task
        })
        print('Load pyspark:')
        print(pyspark)

        with open(pyspark_path, 'w') as f:
            f.write(pyspark)

        load_operator = SparkSubmitOperator(
            task_id='load_{task}'.format(task=task),
            dag=dag,
            name='load_{task}'.format(task=task),
            conf=spark_conf,
            py_files=pyspark_path
        )

        if os.path.isfile(pyspark_path):
            os.remove(pyspark_path)

        wait_sensor >> load_operator

    # Load tasks #

    load_blocks_task = add_load_tasks('blocks', 'json')
    # load_transactions_task = add_load_tasks('transactions', 'json')
    # load_receipts_task = add_load_tasks('receipts', 'json')
    # load_logs_task = add_load_tasks('logs', 'json')
    # load_contracts_task = add_load_tasks('contracts', 'json')
    # load_tokens_task = add_load_tasks('tokens', 'json')
    # load_token_transfers_task = add_load_tasks('token_transfers', 'json')
    # load_traces_task = add_load_tasks('traces', 'json')

    return dag
