import re

from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook


class FixedSparkSubmitHook(SparkSubmitHook):  # noqa
    """
    The `_process_spark_submit_log` method in the SparkSubmitHook, it will track the output logs to determine the status
    of the job, the regex expression is "exit code", but in the latest spark, the end label is "Exit code".
    The class inherits by SparkSubmitHook, overwrite `_process_spark_submit_log` method to enlarge the match scope.
    """

    def __init__(self, *args, **kwargs):
        super(FixedSparkSubmitHook, self).__init__(*args, **kwargs)

    # Override this function in the SparkSubmitHook
    def _process_spark_submit_log(self, itr):
        """
        Processes the log files and extracts useful information out of it.

        If the deploy-mode is 'client', log the output of the submit command as those
        are the output logs of the Spark worker directly.

        Remark: If the driver needs to be tracked for its status, the log-level of the
        spark deploy needs to be at least INFO (log4j.logger.org.apache.spark.deploy=INFO)

        :param itr: An iterator which iterates over the input of the subprocess
        """
        # Consume the iterator
        for line in itr:
            line = line.strip()
            # If we run yarn cluster mode, we want to extract the application id from
            # the logs, so we can kill the application when we stop it unexpectedly
            if self._is_yarn and self._connection['deploy_mode'] == 'cluster':
                match = re.search('(application[0-9_]+)', line)
                if match:
                    self._yarn_application_id = match.groups()[0]
                    self.log.info("Identified spark driver id: %s",
                                  self._yarn_application_id)

            # If we run Kubernetes cluster mode, we want to extract the driver pod id
            # from the logs, so we can kill the application when we stop it unexpectedly
            elif self._is_kubernetes:
                match = re.search(r'\s*pod name: ((.+?)-([a-z0-9]+)-driver)', line)
                if match:
                    self._kubernetes_driver_pod = match.groups()[0]
                    self.log.info("Identified spark driver pod: %s",
                                  self._kubernetes_driver_pod)

                # Store the Spark Exit code
                match_exit_code = re.search(r'\s*[e|E]xit code: (\d+)', line)
                if match_exit_code:
                    self._spark_exit_code = int(match_exit_code.groups()[0])

            # if we run in standalone cluster mode, and we want to track the driver status
            # we need to extract the driver id from the logs. This allows us to poll for
            # the status using the driver id. Also, we can kill the driver when needed.
            elif self._should_track_driver_status and not self._driver_id:
                match_driver_id = re.search(r'(driver-[0-9\-]+)', line)
                if match_driver_id:
                    self._driver_id = match_driver_id.groups()[0]
                    self.log.info("identified spark driver id: {}"
                                  .format(self._driver_id))

            self.log.info(line)
