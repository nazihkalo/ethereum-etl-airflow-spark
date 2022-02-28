from ethereumetl_airflow.operators.spark_submit_operator import SparkSubmitOperator


class SparkSubmitEnrichOperator(SparkSubmitOperator):
    def __init__(self, *args, **kwargs):
        super(SparkSubmitEnrichOperator, self).__init__(*args, **kwargs)

    def _render_sql(self, context):
        _database = self._template_conf['database']
        _database_temp = self._template_conf['database_temp']
        _sql_template_path = self._template_conf['sql_template_path']

        sql_template = self.read_file(_sql_template_path)
        sql = self.render_template(sql_template, {
            'database': _database,
            'database_temp': _database_temp,
            'ds': context['ds'],
            'ds_in_table': context['ds'].replace('-', '_')
        })

        return sql
