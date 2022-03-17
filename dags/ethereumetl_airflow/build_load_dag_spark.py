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
):
    # The following datasets must be created in Spark:
    # - crypto_{chain}_raw
    # - crypto_{chain}

    dataset_name = f'{chain}'
    dataset_name_temp = f'{chain}_raw'
    prices_dataset_name = 'prices'
    prices_dataset_name_temp = 'prices_raw'

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

    common_operator_conf = {
        'dag': dag,
        'conf': spark_conf,
        'bucket': output_bucket,
        'database': dataset_name,
        'database_temp': dataset_name_temp,
        'prices_database': prices_dataset_name,
        'prices_database_temp': prices_dataset_name_temp
    }

    def add_load_tasks(task, file_format='json', task_type='ethereum'):
        _task = f'prices_{task}' if task_type == 'prices' else task

        bucket_file_key = 'export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
            task=_task, datestamp='{{ds}}', file_format=file_format
        )

        wait_sensor = S3KeySensor(
            task_id='wait_latest_{task}'.format(task=_task),
            timeout=60 * 60,
            poke_interval=60,
            bucket_key=bucket_file_key,
            bucket_name=output_bucket,
            dag=dag
        )

        load_operator = SparkSubmitLoadOperator(
            **common_operator_conf,
            task=task,
            task_type=task_type,
            bucket_file_key=bucket_file_key,
            file_format=file_format,
            sql_template_path=os.path.join(
                dags_folder,
                'resources/stages/raw/sqls_spark/{task}.sql'.format(task=_task))
        )

        wait_sensor >> load_operator
        return load_operator

    def add_enrich_tasks(task, dependencies=None, task_type='ethereum'):
        _task = f'prices_{task}' if task_type == 'prices' else task

        enrich_operator = SparkSubmitEnrichOperator(
            **common_operator_conf,
            task=task,
            task_type=task_type,
            sql_template_path=os.path.join(
                dags_folder,
                'resources/stages/enrich/sqls/spark/{task}.sql'.format(task=_task))
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> enrich_operator
        return enrich_operator

    def add_clean_tasks(task, file_format='json', dependencies=None, task_type='ethereum'):
        _task = f'prices_{task}' if task_type == 'prices' else task

        bucket_file_key = 'export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
            task=_task, datestamp='{{ds}}', file_format=file_format
        )

        s3_delete_operator = S3DeleteObjectsOperator(
            task_id='clean_{task}_s3_file'.format(task=_task),
            dag=dag,
            bucket=output_bucket,
            keys=bucket_file_key,
        )

        clean_operator = SparkSubmitCleanOperator(
            **common_operator_conf,
            task=task,
            task_type=task_type,
            sql_template_path=os.path.join(
                dags_folder,
                'resources/stages/enrich/sqls/spark/clean_table.sql')
        )

        # Drop table firstly and then delete data in S3.
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> clean_operator

        clean_operator >> s3_delete_operator

    # Load tasks #
    load_blocks_task = add_load_tasks('blocks')
    load_transactions_task = add_load_tasks('transactions')
    load_receipts_task = add_load_tasks('receipts')
    load_logs_task = add_load_tasks('logs')
    load_token_transfers_task = add_load_tasks('token_transfers')
    load_traces_task = add_load_tasks('traces')
    load_contracts_task = add_load_tasks('contracts')
    load_tokens_task = add_load_tasks('tokens')
    load_prices_usd_task = add_load_tasks('usd', file_format='csv', task_type='prices')

    # Enrich tasks #
    enrich_blocks_task = add_enrich_tasks('blocks', [load_blocks_task])
    enrich_transactions_task = add_enrich_tasks('transactions',
                                                [load_blocks_task, load_transactions_task, load_receipts_task])
    enrich_logs_task = add_enrich_tasks('logs', [load_blocks_task, load_logs_task])
    enrich_token_transfers_task = add_enrich_tasks('token_transfers', [load_blocks_task, load_token_transfers_task])
    enrich_traces_task = add_enrich_tasks('traces', [load_blocks_task, load_traces_task])
    enrich_contracts_task = add_enrich_tasks('contracts', [load_blocks_task, load_contracts_task])
    enrich_tokens_task = add_enrich_tasks('tokens', [load_tokens_task])
    enrich_prices_usd_task = add_enrich_tasks('usd', dependencies=[load_prices_usd_task], task_type='prices')

    # Clean tasks #
    add_clean_tasks('blocks', dependencies=[
        enrich_blocks_task,
        enrich_transactions_task,
        enrich_logs_task,
        enrich_token_transfers_task,
        enrich_traces_task,
        enrich_contracts_task
    ])
    add_clean_tasks('transactions', dependencies=[enrich_transactions_task])
    add_clean_tasks('logs', dependencies=[enrich_logs_task])
    add_clean_tasks('token_transfers', dependencies=[enrich_token_transfers_task])
    add_clean_tasks('traces', dependencies=[enrich_traces_task])
    add_clean_tasks('contracts', dependencies=[enrich_contracts_task])
    add_clean_tasks('tokens', dependencies=[enrich_tokens_task])
    add_clean_tasks('receipts', dependencies=[enrich_transactions_task])
    add_clean_tasks('usd', file_format='csv', dependencies=[enrich_prices_usd_task], task_type='prices')

    return dag
