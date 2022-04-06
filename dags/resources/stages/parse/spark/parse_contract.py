import pyspark.sql.functions as F
from datawaves import new_log_conditions, new_trace_conditions
from pyspark.sql import SparkSession
from spark3 import Spark3

spark = SparkSession.builder.getOrCreate()

spark3 = Spark3(
    spark=spark,
    log=spark.sql('select * from ethereum.logs'),
    log_conditions=new_log_conditions(),
    trace=spark.sql('select * from ethereum.traces'),
    trace_conditions=new_trace_conditions()
)
contract = spark3.contract(address='{{contract_address}}', abi='{{abi}}')

if '{{type}}' == 'event':
    contract.get_event_by_name('{{element_name}}') \
        .select(
        F.col('block_number').alias('evt_block_number'),
        F.col('block_timestamp').alias('evt_block_time'),
        F.col('log_index').alias('evt_index'),
        F.col('transaction_hash').alias('evt_tx_hash'),
        'dt',
        'event_parameter.inputs.*'
    ).createOrReplaceTempView('temp_dt_event_or_function')
elif '{{type}}' == 'call':
    contract.get_function_by_name('{{element_name}}') \
        .withColumn('call_success', F.expr('status==1')) \
        .withColumn('contract_address', F.lit('{{contract_address}}')) \
        .select(
        F.col('block_number').alias('call_block_number'),
        F.col('block_timestamp').alias('call_block_time'),
        F.col('trace_address').alias('call_trace_address'),
        F.col('transaction_hash').alias('call_tx_hash'),
        'call_success',
        'contract_address',
        'dt',
        'function_parameter.inputs.*'
    ).createOrReplaceTempView('temp_dt_event_or_function')
else:
    raise ValueError('the type must be event or function.')

spark.sql("""
    CREATE DATABASE IF NOT EXISTS `{{database}}`
""")

if bool('{{all_partition}}'):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS `{{database}}`.`{{table}}`
        USING parquet
        PARTITIONED BY (dt)
        LOCATION '{{path}}'
        AS SELECT /*+ REPARTITION(1) */
            *
        FROM temp_dt_event_or_function
        WHERE dt <= date '{{ds}}'
    """)
else:
    spark.sql("""
        INSERT OVERWRITE TABLE `{{database}}`.`{{table}}`
            PARTITION (dt = date '{{ds}}')
        SELECT /*+ REPARTITION(1) */
            *
        FROM temp_dt_event_or_function
        WHERE dt = date '{{ds}}'
    """)
