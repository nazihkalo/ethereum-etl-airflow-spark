from ethereumetl_airflow.operators.spark_submit_sql_operator import SparkSubmitSQLOperator


class SparkSubmitEnrichOperator(SparkSubmitSQLOperator):
    def __init__(self,
                 database,
                 database_temp,
                 *args,
                 **kwargs):
        super(SparkSubmitEnrichOperator, self).__init__(*args, **kwargs)

        self._operator_type = 'enrich'
        self._database = database
        self._database_temp = database_temp

    def _get_sql_render_content(self, context):
        return {
            'database': self._database,
            'database_temp': self._database_temp,
            'ds': context['ds'],
            'ds_in_table': context['ds'].replace('-', '_')
        }
