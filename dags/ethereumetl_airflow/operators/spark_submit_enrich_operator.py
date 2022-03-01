import os

from ethereumetl_airflow.operators.spark_submit_operator import SparkSubmitOperator


class SparkSubmitEnrichOperator(SparkSubmitOperator):
    def __init__(self, *args, **kwargs):
        super(SparkSubmitEnrichOperator, self).__init__(*args, **kwargs)

    def _render_pyspark(self, context):
        _task = self._template_conf['task']
        _write_mode = self._template_conf['write_mode']
        _database = self._template_conf['database']
        _database_temp = self._template_conf['database_temp']
        _sql_template_path = self._template_conf['sql_template_path']
        _pyspark_template_path = self._template_conf['pyspark_template_path']

        sql_template = self.read_file(_sql_template_path)
        sql = self.render_template(sql_template, {
            'database': _database,
            'database_temp': _database_temp,
            'ds': context['ds'],
            'ds_in_table': context['ds'].replace('-', '_')
        })

        operator_type = self._template_conf['operator_type']
        pyspark_path = os.path.join(
            '/tmp',
            '{task}_{operator_type}_{ds}.py'.format(task=_task, operator_type=operator_type, ds=context['ds']))
        pyspark_template = self.read_file(_pyspark_template_path)
        pyspark = self.render_template(pyspark_template, {
            'sql': sql,
            'database': _database,
            'table': _task,
            'write_mode': _write_mode
        })
        print('Load pyspark:')
        print(pyspark)

        with open(pyspark_path, 'w') as f:
            f.write(pyspark)

        return 'file://' + pyspark_path
