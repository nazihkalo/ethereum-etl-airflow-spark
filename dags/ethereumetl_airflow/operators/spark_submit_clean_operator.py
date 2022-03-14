from ethereumetl_airflow.operators.spark_submit_sql_operator import SparkSubmitSQLOperator


class SparkSubmitCleanOperator(SparkSubmitSQLOperator):
    def __init__(self,
                 database_temp,
                 *args,
                 **kwargs):
        super(SparkSubmitCleanOperator, self).__init__(*args, **kwargs)

        self._operator_type = 'clean'
        self._database_temp = database_temp

    def _get_sql_render_content(self, context):
        return {
            'database_temp': self._database_temp,
            'table': '{task}_{date}'.format(task=self._task, date=context['ds'].replace('-', '_'))
        }

    def _get_pyspark_render_content(self, context):  # noqa
        return {}
