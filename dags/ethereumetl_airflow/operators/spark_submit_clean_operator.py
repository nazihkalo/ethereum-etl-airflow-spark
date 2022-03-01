import os

from ethereumetl_airflow.operators.spark_submit_operator import SparkSubmitOperator


class SparkSubmitCleanOperator(SparkSubmitOperator):
    def __init__(self, *args, **kwargs):
        super(SparkSubmitCleanOperator, self).__init__(*args, **kwargs)

    def _render_pyspark(self, context):
        _task = self._template_conf['task']
        _database_temp = self._template_conf['database_temp']
        _sql_template_path = self._template_conf['sql_template_path']
        _pyspark_template_path = self._template_conf['pyspark_template_path']

        sql_template = self.read_file(_sql_template_path)
        sql = self.render_template(sql_template, {
            'database_temp': _database_temp,
            'table': '{task}_{date}'.format(task=_task, date=context['ds'].replace('-', '_'))
        })

        operator_type = self._template_conf['operator_type']
        pyspark_path = os.path.join('/tmp', '{task}_{operator_type}.py'.format(task=_task, operator_type=operator_type))
        pyspark_template = self.read_file(_pyspark_template_path)
        pyspark = self.render_template(pyspark_template, {'sql': sql})
        print('Load pyspark:')
        print(pyspark)

        with open(pyspark_path, 'w') as f:
            f.write(pyspark)

        return 'file://' + pyspark_path
