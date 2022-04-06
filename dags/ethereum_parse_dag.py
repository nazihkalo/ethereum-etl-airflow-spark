import logging
import os
from glob import glob

from dags.ethereumetl_airflow.build_parse_dag_spark import build_parse_dag
from dags.ethereumetl_airflow.variables import read_load_dag_spark_vars

DAGS_FOLDER = os.environ.get('DAGS_FOLDER', '/opt/airflow/dags/repo/dags')
table_definitions_folder = os.path.join(DAGS_FOLDER, 'resources/stages/parse/spark/contract_definitions/*')

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

var_prefix = 'ethereum_'

for folder in glob(table_definitions_folder):
    dataset = folder.split('/')[-1]

    dag_id = f'ethereum_parse_{dataset}_dag'
    logging.info(folder)
    logging.info(dataset)
    globals()[dag_id] = build_parse_dag(
        dag_id=dag_id,
        dataset_folder=folder,
        **read_load_dag_spark_vars(
            var_prefix=var_prefix + dataset + '_',
            load_start_date='2022-04-05',
            schedule_interval='0 3 * * *'
        )
    )
