from __future__ import print_function

from ethereumetl_airflow.build_load_dag_spark import build_load_dag_spark
from ethereumetl_airflow.variables import read_load_dag_spark_vars

# airflow DAG
DAG = build_load_dag_spark(
    dag_id='ethereum_load_dag',
    chain='ethereum',
    **read_load_dag_spark_vars(
        var_prefix='ethereum_',
        load_start_date='2022-02-11',
        schedule_interval='30 13 * * *'
    )
)
