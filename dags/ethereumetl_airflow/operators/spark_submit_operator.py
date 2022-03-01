import os

from airflow.models import BaseOperator
from airflow.settings import WEB_COLORS
from airflow.utils.decorators import apply_defaults
from ethereumetl_airflow.operators.new_spark_submit_hook import NewSparkSubmitHook


class SparkSubmitOperator(BaseOperator):
    """
        This hook is a wrapper around the spark-submit binary to kick off a spark-submit job.
        It requires that the "spark-submit" binary is in the PATH or the spark-home is set
        in the extra on the connection.

        :type application: str
        :param conf: Arbitrary Spark configuration properties (templated)
        :type conf: dict
        :param conn_id: The connection id as configured in Airflow administration. When an
                        invalid connection_id is supplied, it will default to yarn.
        :type conn_id: str
        :param files: Upload additional files to the executor running the job, separated by a
                      comma. Files will be placed in the working directory of each executor.
                      For example, serialized objects. (templated)
        :type files: str
        :param driver_class_path: Additional, driver-specific, classpath settings. (templated)
        :type driver_class_path: str
        :param packages: Comma-separated list of maven coordinates of jars to include on the
                         driver and executor classpaths. (templated)
        :type packages: str
        :param exclude_packages: Comma-separated list of maven coordinates of jars to exclude
                                 while resolving the dependencies provided in 'packages' (templated)
        :type exclude_packages: str
        :param repositories: Comma-separated list of additional remote repositories to search
                             for the maven coordinates given with 'packages'
        :type repositories: str
        :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
                                     (Default: all the available cores on the worker)
        :type total_executor_cores: int
        :param executor_cores: (Standalone & YARN only) Number of cores per executor (Default: 2)
        :type executor_cores: int
        :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
        :type executor_memory: str
        :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
        :type driver_memory: str
        :param keytab: Full path to the file that contains the keytab (templated)
        :type keytab: str
        :param principal: The name of the kerberos principal used for keytab (templated)
        :type principal: str
        :param proxy_user: User to impersonate when submitting the application (templated)
        :type proxy_user: str
        :param name: Name of the job (default airflow-spark). (templated)
        :type name: str
        :param num_executors: Number of executors to launch
        :type num_executors: int
        :param status_poll_interval: Seconds to wait between polls of driver status in cluster
            mode (Default: 1)
        :type status_poll_interval: int
        :param application_args: Arguments for the application being submitted (templated)
        :type application_args: list
        :param env_vars: Environment variables for spark-submit. It supports yarn and k8s mode too. (templated)
        :type env_vars: dict
        :param verbose: Whether to pass the verbose flag to spark-submit process for debugging
        :type verbose: bool
        :param spark_binary: The command to use for spark submit.
                             Some distros may use spark2-submit.
        :type spark_binary: str
        """
    template_fields = ('_conf', '_files', '_driver_class_path',
                       '_packages', '_exclude_packages', '_keytab', '_principal', '_proxy_user', '_name',
                       '_application_args', '_env_vars')
    ui_color = WEB_COLORS['LIGHTORANGE']

    @apply_defaults
    def __init__(self,
                 conf=None,
                 conn_id='spark_default',
                 files=None,
                 archives=None,
                 driver_class_path=None,
                 packages=None,
                 exclude_packages=None,
                 repositories=None,
                 total_executor_cores=None,
                 executor_cores=None,
                 executor_memory=None,
                 driver_memory=None,
                 keytab=None,
                 principal=None,
                 proxy_user=None,
                 name='airflow-spark',
                 num_executors=None,
                 status_poll_interval=1,
                 application_args=None,
                 env_vars=None,
                 verbose=False,
                 spark_binary=None,
                 template_conf=None,
                 *args,
                 **kwargs):
        super(SparkSubmitOperator, self).__init__(*args, **kwargs)
        self._conf = conf
        self._files = files
        self._archives = archives
        self._driver_class_path = driver_class_path
        self._packages = packages
        self._exclude_packages = exclude_packages
        self._repositories = repositories
        self._total_executor_cores = total_executor_cores
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._keytab = keytab
        self._principal = principal
        self._proxy_user = proxy_user
        self._name = name
        self._num_executors = num_executors
        self._status_poll_interval = status_poll_interval
        self._application_args = application_args
        self._env_vars = env_vars
        self._verbose = verbose
        self._spark_binary = spark_binary
        self._hook = None
        self._conn_id = conn_id
        self._template_conf = template_conf

    @staticmethod
    def read_file(filepath):
        with open(filepath) as file_handle:
            content = file_handle.read()
            return content

    def _render_pyspark(self, context):
        raise Exception('The function must should be override.')

    def execute(self, context):
        pyspark_path = self._render_pyspark(context)

        """
            Call the SparkSubmitHook to run the provided spark job
        """
        self._hook = NewSparkSubmitHook(
            conf=self._conf,
            conn_id=self._conn_id,
            files=self._files,
            archives=self._archives,
            driver_class_path=self._driver_class_path,
            packages=self._packages,
            exclude_packages=self._exclude_packages,
            repositories=self._repositories,
            total_executor_cores=self._total_executor_cores,
            executor_cores=self._executor_cores,
            executor_memory=self._executor_memory,
            driver_memory=self._driver_memory,
            keytab=self._keytab,
            principal=self._principal,
            proxy_user=self._proxy_user,
            name=self._name,
            num_executors=self._num_executors,
            status_poll_interval=self._status_poll_interval,
            application_args=self._application_args,
            env_vars=self._env_vars,
            verbose=self._verbose,
            spark_binary=self._spark_binary
        )
        self._hook.submit(pyspark_path)

        """
            Clean temp environment
        """
        if os.path.isfile(pyspark_path):
            os.remove(pyspark_path)

    def on_kill(self):
        self._hook.on_kill()
