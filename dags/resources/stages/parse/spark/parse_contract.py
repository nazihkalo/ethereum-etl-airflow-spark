import pyspark.sql.functions as F
from datawaves import new_log_conditions, new_trace_conditions
from pyspark.sql import SparkSession
from spark3 import Spark3

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

spark3 = Spark3(
    spark=spark,
    log=spark.sql('select * from ethereum.logs'),
    log_conditions=new_log_conditions(),
    trace=spark.sql('select * from ethereum.traces'),
    trace_conditions=new_trace_conditions()
)
contract = spark3.contract(address='{{contract_address}}', abi='{{abi}}')

if '{{type}}' == 'event':
    df = contract.get_event_by_name('{{element_name}}') \
        .select(
        F.col('block_number').alias('evt_block_number'),
        F.col('block_timestamp').alias('evt_block_time'),
        F.col('log_index').alias('evt_index'),
        F.col('transaction_hash').alias('evt_tx_hash'),
        'dt',
        'event_parameter.inputs.*'
    )
elif '{{type}}' == 'call':
    df = contract.get_function_by_name('{{element_name}}') \
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
    )
else:
    raise ValueError('the type must be event or function.')

df.createOrReplaceTempView('temp_dt_event_or_function')

spark.sql("""
    CREATE DATABASE IF NOT EXISTS `{{database}}`
""")

candidates = [i for i in spark.sql("show tables in {{database}}").collect() if
              i['tableName'] == '{{table}}'.lower() and not i['isTemporary']]

if len(candidates) == 0:
    df.filter(F.expr("dt <= date '{{ds}}'")) \
        .repartition('dt') \
        .write \
        .partitionBy('dt') \
        .option('path', '{{path}}') \
        .format('parquet') \
        .saveAsTable('{{database}}.{{table}}')
else:
    df.filter(F.expr("dt == date '{{ds}}'")) \
        .repartition(1) \
        .select(spark.table('{{database}}.{{table}}').schema.fieldNames()) \
        .write \
        .mode('overwrite') \
        .insertInto('{{database}}.{{table}}')
