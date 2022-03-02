from ethereumetl_airflow.operators.spark_submit_operator import SparkSubmitOperator


class SparkSubmitLoadOperator(SparkSubmitOperator):
    def __init__(self, *args, **kwargs):
        super(SparkSubmitLoadOperator, self).__init__(*args, **kwargs)

    def _get_sql_render_content(self, context):
        _task = self._template_conf['task']
        return {
            'database': self._template_conf['database'],
            'table': '{task}_{date}'.format(task=_task, date=context['ds'].replace('-', '_')),
            'file_path': 's3a://{bucket}/{bucket_name}'.format(
                bucket=self._template_conf['bucket'],
                bucket_name='export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
                    task=_task, datestamp=context['ds'], file_format=self._template_conf['file_format']
                )
            )
        }

    def _get_pyspark_render_content(self, context):
        return {}
