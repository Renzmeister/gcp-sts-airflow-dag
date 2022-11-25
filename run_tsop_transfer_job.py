import datetime
import logging
import time

from airflow import models, utils as airflow_utils
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import googleapiclient.discovery
from oauth2client.client import GoogleCredentials



PROJECT_ID = <sts_job_project_id>
TSOP_JOB_NAME = <transfer_job_name>

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': airflow_utils.dates.days_ago(7),
}


class TSOPJobRunOperator(BaseOperator):
  """Runs the TSOP job."""

  @apply_defaults
  def __init__(self, project, job_name, status_poll_interval: int = 5,  *args, **kwargs):
    self._tsop = None
    self.project = project
    self.job_name = job_name
    self._status_poll_interval = status_poll_interval
    super(TSOPJobRunOperator, self).__init__(*args, **kwargs)

  def get_tsop_api_client(self):
    if self._tsop is None:
      credentials = GoogleCredentials.get_application_default()
      self._tsop = googleapiclient.discovery.build(
        'storagetransfer', 'v1', cache_discovery=False,       
        credentials=credentials)
    return self._tsop

  def _start_tsop_job_status_tracking(self) -> None:
    """
    Polls the transfer job's status periodically. 
    Finish successfully when the status is SUCCESS.
    Finish failed when the status is FAILED/ABORTED
    """

    while self._job_status not in ["SUCCESS", "FAILED", "ABORTED"]:
      time.sleep(self._status_poll_interval)
      logging.info(f"Checking status of job {self.job_name}")
      self._job_status = self.get_tsop_api_client().transferOperations().list(
        filter=f"{{'project_id': '{self.project}', jobNames: ['{self.job_name}']}}",
        name="transferOperations"
      ).execute()["operations"][0]["metadata"]["status"]
      logging.info(f"Job status is {self._job_status}")


  def execute(self, context):
    logging.info('Running Job %s in project %s',
                 self.job_name, self.project)
    self.get_tsop_api_client().transferJobs().run(
      jobName=self.job_name,
      body={'project_id': self.project}).execute()

    # Initial job status will be "IN_PROGRESS"
    self._job_status = "IN_PROGRESS"

    self._start_tsop_job_status_tracking()

    if self._job_status != "SUCCESS":
      raise AirflowException(
        f"ERROR: Transfer job did not complete successfully. Job status is {self._job_status}. Please check the console for the logs."
      )


with models.DAG(
    "run_tsop_transfer_job",
    default_args=DEFAULT_ARGS,
    schedule_interval='*/10 * * * *',
    max_active_runs=1,
    tags=["tsop_job"],
    user_defined_macros={"TSOP_JOB_NAME": TSOP_JOB_NAME},
) as dag:
    run_tsop_job = TSOPJobRunOperator(
        project=PROJECT_ID, job_name=TSOP_JOB_NAME, 
        task_id='run_tsop_job')