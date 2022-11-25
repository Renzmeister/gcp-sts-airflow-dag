# gcp-sts-airflow-dag

This is the Airflow DAG originally created by Pradeep Kumar Singh and outlined in his [Medium article](https://medium.com/google-cloud/schedule-gcp-sts-transfer-job-with-cloud-composer-2aa947fb1948). I've attempted to improve the Operator by adding a periodic check for the status of the transfer job, so that the task run doesn't exit with status Succeeded while the transfer job might be in a different state (such as still running, or failed.)
