import boto3
import time

emr = boto3.client('emr', region_name='ap-south-1')

CLUSTER_NAME = "aakash_spark_cluster"
LOG_URI = "s3://spark-bucket-aakash/spark_logs/"

BOOTSTRAP_ACTIONS = [
    {
        'Name': 'Setup log4j and upload logs to S3',
        'ScriptBootstrapAction': {
            'Path': 's3://spark-bucket-aakash/bootstrap/bootstrap.sh',
            'Args': []
        }
    }
]

SPARK_STEP = {
    'Name': 'Run pyspark testing with log job',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
            '--conf', 'spark.driver.extraJavaOptions=-Dlog4j.configurationFile=file:///tmp/log4j.properties',
            '--conf', 'spark.executor.extraJavaOptions=-Dlog4j.configurationFile=file:///tmp/log4j.properties',
            's3://spark-bucket-aakash/scripts/test1.py'
        ]
    }
}

UPLOAD_LOG_STEP = {
    'Name': 'Upload Spark log to S3',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'bash', '-c',
            'if [ -f /tmp/logs/spark_app.log ]; then '
            'aws s3 cp /tmp/logs/spark_app.log s3://spark-bucket-aakash/spark_logs/spark_app-$(date +%d-%m-%y-%H-%M-%S).log;'
            'else echo "Log file not found, skipping upload."; fi'
        ]
    }
}

def create_cluster():
    response = emr.run_job_flow(
        Name=CLUSTER_NAME,
        LogUri=LOG_URI,
        ReleaseLabel='emr-7.8.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Task nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'TASK',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-0df634487a714c648',
            'EmrManagedMasterSecurityGroup': 'sg-0576997556757ab4d',
            'EmrManagedSlaveSecurityGroup': 'sg-0525d2760714aae89',
        },
        BootstrapActions=BOOTSTRAP_ACTIONS,
        Applications=[{'Name': 'Spark'}],
        JobFlowRole='AmazonEMR-InstanceProfile-20250515T181538',
        ServiceRole='AmazonEMR-ServiceRole-Aakash',
        VisibleToAllUsers=True
    )
    return response['JobFlowId']

def wait_for_cluster(cluster_id):
    print(f"Waiting for cluster {cluster_id} to be in WAITING state...")
    while True:
        response = emr.describe_cluster(ClusterId=cluster_id)
        state = response['Cluster']['Status']['State']
        print(f"Cluster state: {state}")
        if state == 'WAITING':
            print("Cluster is ready!")
            break
        elif state in ('TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'):
            reason = response['Cluster']['Status']['StateChangeReason']['Message']
            raise Exception(f"Cluster terminated early with state: {state}. Reason: {reason}")
        time.sleep(30)

def add_spark_steps(cluster_id):
    step_response = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[SPARK_STEP, UPLOAD_LOG_STEP]
    )
    return step_response['StepIds']

def wait_for_step(cluster_id, step_id):
    while True:
        step_status = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = step_status['Step']['Status']['State']
        print(f"Step state: {state}")
        if state in ['COMPLETED', 'FAILED', 'CANCELLED']:
            return state
        time.sleep(30)

def terminate_cluster(cluster_id):
    emr.terminate_job_flows(JobFlowIds=[cluster_id])

if __name__ == "__main__":
    cluster_id = create_cluster()
    print(f"Created cluster {cluster_id}...")

    wait_for_cluster(cluster_id)
    step_ids = add_spark_steps(cluster_id)

    print(f"Waiting for Spark step {step_ids[0]} to complete...")
    wait_for_step(cluster_id, step_ids[0])

    print(f"Waiting for log upload step {step_ids[1]} to complete...")
    wait_for_step(cluster_id, step_ids[1])

    print("Terminating cluster...")
    terminate_cluster(cluster_id)
    print("Cluster terminated.")
