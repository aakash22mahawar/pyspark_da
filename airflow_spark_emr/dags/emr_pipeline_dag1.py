from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import  EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta


# Define default arguments for the DAG
default_args = {
    "owner": "aakash",
    "start_date": datetime(2024, 1, 1),
    "retries": None,
    "retry_delay": None,
}

# Define cluster configuration
JOB_FLOW_OVERRIDES = {
    "Name": "aakash_spark_cluster",
    "ReleaseLabel": "emr-7.8.0",
    "LogUri": "s3://spark-bucket-aakash/spark_logs/",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Task nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "TASK",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "Ec2SubnetId": "subnet-0df634487a714c648",
        "EmrManagedMasterSecurityGroup": "sg-0576997556757ab4d",
        "EmrManagedSlaveSecurityGroup": "sg-0525d2760714aae89",
    },
    "BootstrapActions": [
        {
            "Name": "Setup log4j and create logs folder",
            "ScriptBootstrapAction": {
                "Path": "s3://spark-bucket-aakash/bootstrap/bootstrap.sh",
                "Args": []
            }
        }
    ],
    "JobFlowRole": "AmazonEMR-InstanceProfile-20250515T181538",
    "ServiceRole": "AmazonEMR-ServiceRole-Aakash",
    "VisibleToAllUsers": True,
}

# define steps
SPARK_STEP = {
    'Name': 'Run pyspark 1M data with logs job',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
            '--conf', 'spark.driver.extraJavaOptions=-Dlog4j.configurationFile=file:///tmp/log4j.properties',
            '--conf', 'spark.executor.extraJavaOptions=-Dlog4j.configurationFile=file:///tmp/log4j.properties',
            's3://spark-bucket-aakash/scripts/1m_emr.py'
        ]
    }
}

UPLOAD_LOG_STEP = {
    'Name': 'Upload Spark logs to S3',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'bash', '-c',
            'if [ -f /tmp/logs/spark_app.log ]; then '
            'aws s3 cp /tmp/logs/spark_app.log s3://spark-bucket-aakash/spark_logs/logs/spark_app-$(date +%d-%m-%y-%H-%M-%S).log;'
            'else echo "Log file not found, skipping upload."; fi'
        ]
    }
}


# Define the DAG
with DAG(
    dag_id="emr_spark_pipeline_provider",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["emr", "spark", "provider"],
) as dag:

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws",
        dag = dag
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_spark_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=[SPARK_STEP, UPLOAD_LOG_STEP],
        aws_conn_id="aws",
        dag=dag
    )

    wait_for_spark_step = EmrStepSensor(
        task_id="wait_for_spark_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps')[0] }}",
        aws_conn_id="aws",
        dag=dag
    )

    wait_for_logs_upload_step = EmrStepSensor(
        task_id="wait_for_logs_upload",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps')[1] }}",
        aws_conn_id="aws",
        dag=dag
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws",
        trigger_rule=TriggerRule.ALL_DONE  # So it still terminates on step failure
    )

    create_cluster >> add_steps >> wait_for_spark_step >> wait_for_logs_upload_step >> terminate_cluster

