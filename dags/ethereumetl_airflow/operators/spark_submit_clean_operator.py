from ethereumetl_airflow.operators.spark_submit_operator import SparkSubmitOperator


class SparkSubmitCleanOperator(SparkSubmitOperator):
    def __init__(self, *args, **kwargs):
        super(SparkSubmitCleanOperator, self).__init__(*args, **kwargs)

    def _render_sql(self, context):
        _task = self._template_conf['task']
        _database_temp = self._template_conf['database_temp']
        _sql_template_path = self._template_conf['sql_template_path']

        sql_template = self.read_file(_sql_template_path)
        sql = self.render_template(sql_template, {
            'database_temp': _database_temp,
            'table': '{task}_{date}'.format(task=_task, date=context['ds'].replace('-', '_'))
        })
        
        return sql
