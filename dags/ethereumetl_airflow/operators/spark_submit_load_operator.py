from ethereumetl_airflow.operators.spark_submit_sql_operator import SparkSubmitSQLOperator


class SparkSubmitLoadOperator(SparkSubmitSQLOperator):
    def __init__(self,
                 bucket,
                 database,
                 file_format,
                 *args,
                 **kwargs):
        super(SparkSubmitLoadOperator, self).__init__(*args, **kwargs)
        self._operator_type = 'load'
        self._bucket = bucket
        self._database = database
        self._file_format = file_format

    def _get_sql_render_content(self, context):
        return {
            'database': self._database,
            'table': '{task}_{date}'.format(task=self._task, date=context['ds'].replace('-', '_')),
            'file_path': 's3a://{bucket}/{bucket_name}'.format(
                bucket=self._bucket,
                bucket_name='export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
                    task=self._task, datestamp=context['ds'], file_format=self._file_format
                )
            )
        }

    def _get_pyspark_render_content(self, context):  # noqa
        return {}
