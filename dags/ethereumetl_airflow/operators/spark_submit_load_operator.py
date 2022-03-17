from ethereumetl_airflow.operators.spark_submit_sql_operator import SparkSubmitSQLOperator


class SparkSubmitLoadOperator(SparkSubmitSQLOperator):
    def __init__(self,
                 bucket,
                 database_temp,
                 prices_database_temp,
                 file_format,
                 *args,
                 **kwargs):
        super(SparkSubmitLoadOperator, self).__init__(operator_type='load', *args, **kwargs)
        self._bucket = bucket
        self._database_temp = database_temp
        self._prices_database_temp = prices_database_temp
        self._file_format = file_format

    def _get_sql_render_context(self, context):
        return {
            'database_temp': self._prices_database_temp if self._task_type == 'prices' else self._database_temp,
            'table': '{task}_{date}'.format(task=self._task, date=context['ds'].replace('-', '_')),
            'file_format': self._file_format,
            'file_path': 's3a://{bucket}/{bucket_name}'.format(
                bucket=self._bucket,
                bucket_name='export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
                    task=f'prices_{self._task}' if self._task_type == 'prices' else self._task,
                    datestamp=context['ds'],
                    file_format=self._file_format
                )
            )
        }
