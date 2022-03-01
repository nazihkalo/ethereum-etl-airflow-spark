import os

from ethereumetl_airflow.operators.spark_submit_operator import SparkSubmitOperator


class SparkSubmitLoadOperator(SparkSubmitOperator):
    def __init__(self, *args, **kwargs):
        super(SparkSubmitLoadOperator, self).__init__(*args, **kwargs)

    def _render_pyspark(self, context):
        _task = self._template_conf['task']
        _bucket = self._template_conf['bucket']
        _database = self._template_conf['database']
        _file_format = self._template_conf['file_format']
        _sql_template_path = self._template_conf['sql_template_path']
        _pyspark_template_path = self._template_conf['pyspark_template_path']

        sql_template = self.read_file(_sql_template_path)
        sql = self.render_template(sql_template, {
            'database': _database,
            'table': '{task}_{date}'.format(task=_task, date=context['ds'].replace('-', '_')),
            'file_path': 's3a://{bucket}/{bucket_name}'.format(
                bucket=_bucket,
                bucket_name='export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
                    task=_task, datestamp=context['ds'], file_format=_file_format
                )
            )
        })

        operator_type = self._template_conf['operator_type']
        pyspark_path = os.path.join(
            '/tmp',
            '{task}_{operator_type}_{ds}.py'.format(task=_task, operator_type=operator_type, ds=context['ds']))
        pyspark_template = self.read_file(_pyspark_template_path)
        pyspark = self.render_template(pyspark_template, {'sql': sql})
        print('Load pyspark:')
        print(pyspark)

        with open(pyspark_path, 'w') as f:
            f.write(pyspark)

        return 'file://' + pyspark_path
