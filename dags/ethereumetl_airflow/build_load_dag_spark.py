import logging
import os
from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators.s3_delete_objects_operator import S3DeleteObjectsOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from ethereumetl_airflow.operators.spark_submit_clean_operator import SparkSubmitCleanOperator
from ethereumetl_airflow.operators.spark_submit_enrich_operator import SparkSubmitEnrichOperator
from ethereumetl_airflow.operators.spark_submit_load_operator import SparkSubmitLoadOperator

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
    dataset_name_temp = f'{chain}_raw'

    if not spark_conf:
        raise ValueError('k8s_config is required')

    # TODO: the retries number and retry delay for test
    default_dag_args = {
        'depends_on_past': False,
        'start_date': load_start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
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

        load_operator = SparkSubmitLoadOperator(
            task_id='load_{task}'.format(task=task),
            dag=dag,
            name='load_{task}'.format(task=task),
            conf=spark_conf,
            template_conf={
                'task': task,
                'bucket': output_bucket,
                'database': dataset_name_temp,
                'operator_type': 'load',
                'file_format': file_format,
                'sql_template_path': os.path.join(
                    dags_folder,
                    'resources/stages/raw/sqls_spark/{task}.sql'.format(task=task)),
                'pyspark_template_path': os.path.join(
                    dags_folder,
                    'resources/stages/spark/spark_sql.py.template')
            }
        )

        wait_sensor >> load_operator
        return load_operator

    def add_enrich_tasks(task, dependencies=None):
        enrich_operator = SparkSubmitEnrichOperator(
            task_id='enrich_{task}'.format(task=task),
            dag=dag,
            name='enrich_{task}'.format(task=task),
            conf=spark_conf,
            template_conf={
                'task': task,
                'database': dataset_name,
                'operator_type': 'enrich',
                'database_temp': dataset_name_temp,
                'sql_template_path': os.path.join(
                    dags_folder,
                    'resources/stages/enrich/sqls/spark/{task}.sql'.format(task=task)),
                'pyspark_template_path': os.path.join(
                    dags_folder,
                    'resources/stages/spark/insert_into_table.py.template')
            }
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> enrich_operator
        return enrich_operator

    def add_clean_tasks(task, file_format, have_temp_table=False, dependencies=None):
        bucket_file_key = 'export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
            task=task, datestamp='{{ds}}', file_format=file_format
        )

        s3_delete_operator = S3DeleteObjectsOperator(
            bucket=output_bucket,
            keys=bucket_file_key,
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> s3_delete_operator

        if have_temp_table:
            clean_operator = SparkSubmitCleanOperator(
                task_id='clean_{task}'.format(task=task),
                dag=dag,
                name='clean_{task}'.format(task=task),
                conf=spark_conf,
                template_conf={
                    'task': task,
                    'operator_type': 'clean',
                    'database_temp': dataset_name_temp,
                    'sql_template_path': os.path.join(
                        dags_folder,
                        'resources/stages/enrich/sqls/spark/clean_table.sql'),
                    'pyspark_template_path': os.path.join(
                        dags_folder,
                        'resources/stages/spark/spark_sql.py.template')
                }
            )

            s3_delete_operator >> clean_operator

    # Load tasks #
    load_blocks_task = add_load_tasks('blocks', 'json')
    load_transactions_task = add_load_tasks('transactions', 'json')
    load_receipts_task = add_load_tasks('receipts', 'json')
    load_logs_task = add_load_tasks('logs', 'json')
    load_token_transfers_task = add_load_tasks('token_transfers', 'json')
    load_traces_task = add_load_tasks('traces', 'json')
    load_contracts_task = add_load_tasks('contracts', 'json')
    # load_tokens_task = add_load_tasks('tokens', 'json')

    # Enrich tasks #
    enrich_blocks_task = add_enrich_tasks(
        'blocks', dependencies=[load_blocks_task])
    enrich_transactions_task = add_enrich_tasks(
        'transactions', dependencies=[load_blocks_task, load_transactions_task, load_receipts_task])
    enrich_logs_task = add_enrich_tasks(
        'logs', dependencies=[load_blocks_task, load_logs_task])
    enrich_token_transfers_task = add_enrich_tasks(
        'token_transfers', dependencies=[load_blocks_task, load_token_transfers_task])
    enrich_traces_task = add_enrich_tasks(
        'traces', dependencies=[load_blocks_task, load_traces_task])
    enrich_contracts_task = add_enrich_tasks(
        'contracts', dependencies=[load_blocks_task, load_contracts_task])
    # enrich_tokens_task = add_enrich_tasks(
    #     'tokens', dependencies=[load_tokens_task])

    # Clean tasks #
    add_clean_tasks('blocks', 'json', True, dependencies=[enrich_blocks_task])
    add_clean_tasks('transactions', 'json', True, dependencies=[enrich_transactions_task])
    add_clean_tasks('logs', 'json', True, dependencies=[enrich_logs_task])
    add_clean_tasks('token_transfers', 'json', True, dependencies=[enrich_token_transfers_task])
    add_clean_tasks('traces', 'json', True, dependencies=[enrich_traces_task])
    add_clean_tasks('contracts', 'json', True, dependencies=[enrich_contracts_task])
    # add_clean_tasks('tokens', 'json', True, dependencies=[enrich_tokens_task])
    add_clean_tasks('receipts', 'json', False, dependencies=[load_receipts_task])

    return dag
