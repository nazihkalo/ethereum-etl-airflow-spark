from ethereumetl_airflow.operators.spark_submit_operator import SparkSubmitOperator


class SparkSubmitEnrichOperator(SparkSubmitOperator):
    def __init__(self, *args, **kwargs):
        super(SparkSubmitEnrichOperator, self).__init__(*args, **kwargs)

    def _get_sql_render_content(self, context):
        return {
            'database': self._template_conf['database'],
            'database_temp': self._template_conf['database_temp'],
            'ds': context['ds'],
            'ds_in_table': context['ds'].replace('-', '_')
        }

    def _get_pyspark_render_content(self, context):
        return {
            'table': self._template_conf['task'],
            'database': self._template_conf['database'],
            'write_mode': self._template_conf['write_mode']
        }
