import logging
import os
from datetime import datetime, timedelta
from glob import glob
from typing import cast, List, Dict

from airflow import models
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.sensors import ExternalTaskSensor
from bdbt.abi.abi_type import ABI
from ethereumetl_airflow.common import read_json_file
from ethereumetl_airflow.operators.spark_sumbit_py_operator import SparkSubmitPyOperator

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

dags_folder = os.environ.get('DAGS_FOLDER', '/opt/airflow/dags/repo/dags')


class ContractDefinition(TypedDict, total=False):
    abi: ABI
    contract_address: str
    contract_name: str
    dataset_name: str


def build_parse_dag(
        dag_id: str,
        dataset_folder: str,
        output_bucket: str,
        notification_emails: str = None,
        spark_conf: Dict[str, any] = None,
        parse_start_date: datetime = datetime(2018, 7, 1),
        schedule_interval: str = '0 0 * * *'
):
    default_dag_args = {
        'depends_on_past': True,
        'start_date': parse_start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    dag = models.DAG(
        dag_id,
        catchup=False,
        schedule_interval=schedule_interval,
        default_args=default_dag_args)

    common_operator_conf = {
        'dag': dag,
        'conf': spark_conf,
    }

    def get_enrich_table_sensor(table_name: str) -> ExternalTaskSensor:
        if table_name != 'logs' and table_name != 'traces':
            raise ValueError('table name must be logs or traces.')

        return ExternalTaskSensor(
            task_id=f'wait_for_ethereum_enrich_{table_name}',
            external_dag_id='ethereum_load_dag',
            external_task_id=f'enrich_{table_name}',
            execution_delta=timedelta(minutes=30),
            priority_weight=0,
            mode='reschedule',
            poke_interval=5 * 60,
            timeout=60 * 60 * 12,
            dag=dag
        )

    # Create multiply parse tasks for one contract
    def create_parse_tasks(contract: ContractDefinition):
        for element in contract['abi']:
            etype = element['type']
            table_name = f'{contract["contract_name"]}_{"evt" if etype == "event" else "call"}_{element["name"]}'
            path = f's3a://{output_bucket}/ethereumetl/enrich/{contract["dataset_name"]}/{table_name}'
            parse_all_partition = not check_table_exists(
                bucket=output_bucket,
                dataset=contract['dataset_name'],
                table=table_name
            )
            operator = SparkSubmitPyOperator(
                **common_operator_conf,
                table_name=table_name,
                dataset_name=contract['dataset_name'],
                render_context={
                    'type': etype,
                    'contract_address': contract['contract_address'],
                    'abi': contract['abi'],
                    'element_name': element['name'],
                    'path': path,
                    'parse_all_partition': parse_all_partition
                }
            )

            dependency = get_enrich_table_sensor('logs' if etype == 'event' else 'traces')
            dependency >> operator

    json_files = get_list_of_files(dataset_folder, '*.json')
    logging.info(json_files)

    for json_file in json_files:
        table_definition = cast(read_json_file(json_file), ContractDefinition)
        create_parse_tasks(table_definition)


def check_table_exists(
        bucket: str,
        dataset: str,
        table: str,
        aws_conn_id: str = 'aws_default'
) -> bool:
    bucket_key = f'ethereumetl/enrich/{dataset}/{table}/_SUCCESS'
    hook = S3Hook(aws_conn_id=aws_conn_id)
    logging.info('Poking for key : s3://%s/%s', bucket, bucket_key)
    return hook.check_for_key(bucket_key, bucket)


def get_list_of_files(dataset_folder: str, filter: str = '*.json') -> List[str]:
    logging.info('get_list_of_files')
    logging.info(dataset_folder)
    logging.info(os.path.join(dataset_folder, filter))
    return [f for f in glob(os.path.join(dataset_folder, filter))]
