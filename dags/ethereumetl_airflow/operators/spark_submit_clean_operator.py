from ethereumetl_airflow.operators.spark_submit_sql_operator import SparkSubmitSQLOperator


class SparkSubmitCleanOperator(SparkSubmitSQLOperator):
    def __init__(self,
                 database_temp,
                 *args,
                 **kwargs):
        super(SparkSubmitCleanOperator, self).__init__(operator_type='clean', *args, **kwargs)
        self._database_temp = database_temp

    def _get_sql_render_context(self, context):
        return {
            'database_temp': self._database_temp,
            'table': '{task}_{date}'.format(task=self._task, date=context['ds'].replace('-', '_'))
        }
