import boto3
import time

emr = boto3.client('emr', region_name='ap-south-1')  # update region

CLUSTER_NAME = "aakash_spark_cluster"
LOG_URI = "s3://spark-bucket-aakash/spark_logs/"
BOOTSTRAP_ACTIONS = [
    {
        'Name': 'Download log4j properties',
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


def create_cluster():
    response = emr.run_job_flow(
        Name=CLUSTER_NAME,
        LogUri=LOG_URI,
        ReleaseLabel='emr-7.8.0',  # or your preferred version
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
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': 'Core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': 'Task nodes',
                    'Market': 'ON_DEMAND',  # or 'SPOT' if cost-sensitive
                    'InstanceRole': 'TASK',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-0df634487a714c648',  # your subnet id
            'EmrManagedMasterSecurityGroup': 'sg-0576997556757ab4d',  # your security groups
            'EmrManagedSlaveSecurityGroup': 'sg-0525d2760714aae89',
        },
        BootstrapActions=BOOTSTRAP_ACTIONS,
        Applications=[{'Name': 'Spark'}],
        JobFlowRole='AmazonEMR-InstanceProfile-20250515T181538',  # Ensure proper IAM roles
        ServiceRole='AmazonEMR-ServiceRole-20250515T181557',
        VisibleToAllUsers=True
    )
    return response['JobFlowId']


def add_spark_step(cluster_id):
    step_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[SPARK_STEP])
    return step_response['StepIds'][0]


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
    print(f"Created cluster {cluster_id}, waiting for cluster to be ready...")
    time.sleep(300)  # simple wait, can improve with DescribeCluster API to check status

    step_id = add_spark_step(cluster_id)
    print(f"Added spark step {step_id}, waiting for completion...")
    step_state = wait_for_step(cluster_id, step_id)
    print(f"Step finished with state: {step_state}")

    print("Terminating cluster...")
    terminate_cluster(cluster_id)
    print("Cluster terminated.")
