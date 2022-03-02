from ethereumetl_airflow.operators.spark_submit_operator import SparkSubmitOperator


class SparkSubmitCleanOperator(SparkSubmitOperator):
    def __init__(self, *args, **kwargs):
        super(SparkSubmitCleanOperator, self).__init__(*args, **kwargs)

    def _get_sql_render_content(self, context):
        return {
            'database_temp': self._template_conf['database_temp'],
            'table': '{task}_{date}'.format(task=self._template_conf['task'], date=context['ds'].replace('-', '_'))
        }

    def _get_pyspark_render_content(self, context):
        return {}
