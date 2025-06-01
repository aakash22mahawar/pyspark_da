from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from emr_submit_1m import *

default_args = {
    "owner": "aakash",
    "start_date": datetime.now(),
    "retries": 0,
    "retry_delay": None,
}

with DAG(
    dag_id='emr_spark_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['emr', 'spark', 'ci_cd'],
) as dag:

    def create_cluster_task(**kwargs):
        return create_cluster()  # Pushes via XCom automatically

    def wait_for_cluster_task(**kwargs):
        cluster_id = kwargs['ti'].xcom_pull(task_ids='create_cluster')
        wait_for_cluster(cluster_id)

    def add_steps_task(**kwargs):
        cluster_id = kwargs['ti'].xcom_pull(task_ids='create_cluster')
        return add_spark_steps(cluster_id)

    def wait_for_spark_step_task(**kwargs):
        cluster_id = kwargs['ti'].xcom_pull(task_ids='create_cluster')
        step_ids = kwargs['ti'].xcom_pull(task_ids='add_spark_steps')
        wait_for_step(cluster_id, step_ids[0])

    def wait_for_upload_step_task(**kwargs):
        cluster_id = kwargs['ti'].xcom_pull(task_ids='create_cluster')
        step_ids = kwargs['ti'].xcom_pull(task_ids='add_spark_steps')
        wait_for_step(cluster_id, step_ids[1])

    def terminate_cluster_task(**kwargs):
        cluster_id = kwargs['ti'].xcom_pull(task_ids='create_cluster')
        terminate_cluster(cluster_id)

    t1 = PythonOperator(
        task_id='create_cluster',
        python_callable=create_cluster_task
    )

    t2 = PythonOperator(
        task_id='wait_for_cluster_ready',
        python_callable=wait_for_cluster_task
    )

    t3 = PythonOperator(
        task_id='add_spark_steps',
        python_callable=add_steps_task
    )

    t4 = PythonOperator(
        task_id='wait_for_spark_step',
        python_callable=wait_for_spark_step_task
    )

    t5 = PythonOperator(
        task_id='wait_for_log_upload',
        python_callable=wait_for_upload_step_task
    )

    t6 = PythonOperator(
        task_id='terminate_cluster',
        python_callable=terminate_cluster_task
    )

    # DAG dependencies
    t1 >> t2 >> t3 >> t4 >> t5 >> t6

