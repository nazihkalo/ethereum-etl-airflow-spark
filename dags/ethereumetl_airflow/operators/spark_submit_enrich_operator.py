from ethereumetl_airflow.operators.spark_submit_sql_operator import SparkSubmitSQLOperator


class SparkSubmitEnrichOperator(SparkSubmitSQLOperator):
    def __init__(self,
                 database,
                 database_temp,
                 prices_database,
                 prices_database_temp,
                 *args,
                 **kwargs):
        super(SparkSubmitEnrichOperator, self).__init__(operator_type='enrich', *args, **kwargs)
        self._database = database
        self._database_temp = database_temp
        self._prices_database = prices_database
        self._prices_database_temp = prices_database_temp

    def _get_sql_render_context(self, context):
        return {
            'database': self._prices_database if self._task == 'usd' else self._database,
            'database_temp': self._prices_database_temp if self._task == 'usd' else self._database_temp,
            'ds': context['ds'],
            'ds_in_table': context['ds'].replace('-', '_')
        }
