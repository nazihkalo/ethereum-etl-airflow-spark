import json
import logging
import os
from datetime import datetime, timedelta
from glob import glob
from typing import cast, List, Dict

from airflow import models, DAG
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
) -> DAG:
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

    # Create multiply parse tasks for one contract
    def create_parse_tasks(
            contract: ContractDefinition,
            logs_sensor: ExternalTaskSensor,
            traces_sensor: ExternalTaskSensor
    ):
        for element in contract['abi']:
            etype = element['type']

            if etype != 'event' and etype != 'function' or len(element.get('inputs', [])) == 0:
                continue

            table_name = f'{contract["contract_name"]}_{"evt" if etype == "event" else "call"}_{element["name"]}'
            path = f's3a://{output_bucket}/ethereumetl/enrich/{contract["dataset_name"]}/{table_name}'
            operator = SparkSubmitPyOperator(
                dag=dag,
                conf=spark_conf['conf'],
                jars=spark_conf['jars'],
                py_files=spark_conf['py_files'],
                table_name=table_name,
                dataset_name=contract['dataset_name'],
                render_context={
                    'type': 'event' if etype == 'event' else 'call',
                    'contract_address': contract['contract_address'].lower(),
                    'abi': json.dumps(contract['abi']),
                    'element_name': element['name'],
                    'path': path
                }
            )

            dependency = logs_sensor if etype == 'event' else traces_sensor
            dependency >> operator

    logs_sensor = ExternalTaskSensor(
        task_id=f'wait_for_ethereum_enrich_logs',
        external_dag_id='ethereum_load_dag',
        external_task_id=f'enrich_logs',
        execution_delta=timedelta(hours=1),
        priority_weight=0,
        mode='reschedule',
        poke_interval=5 * 60,
        timeout=60 * 60 * 12,
        dag=dag
    )

    traces_sensor = ExternalTaskSensor(
        task_id=f'wait_for_ethereum_enrich_traces',
        external_dag_id='ethereum_load_dag',
        external_task_id=f'enrich_traces',
        execution_delta=timedelta(hours=1),
        priority_weight=0,
        mode='reschedule',
        poke_interval=5 * 60,
        timeout=60 * 60 * 12,
        dag=dag
    )

    json_files = get_list_of_files(dataset_folder, '*.json')
    logging.info(json_files)

    for json_file in json_files:
        table_definition = cast(ContractDefinition, read_json_file(json_file))
        create_parse_tasks(table_definition, logs_sensor, traces_sensor)

    return dag


def get_list_of_files(dataset_folder: str, filter: str = '*.json') -> List[str]:
    logging.info('get_list_of_files')
    logging.info(dataset_folder)
    logging.info(os.path.join(dataset_folder, filter))
    return [f for f in glob(os.path.join(dataset_folder, filter))]
