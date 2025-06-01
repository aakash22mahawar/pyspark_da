from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    "owner": "aakash",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id='python_dag',
    default_args=default_args,
    description='PythonOperator DAG with XComs',
    schedule=None,
    catchup=False,
    tags=["example"]
) as dag:

    # Task 1: Print a message
    def print_message():
        print("Hello from the PythonOperator!")

    task_print_message = PythonOperator(
        task_id='print_message',
        python_callable=print_message,
    )

    # Task 2: Push values via XCom
    def xcoms_push(ti):
        ti.xcom_push(key='num1', value=25)
        ti.xcom_push(key='num2', value=100)

    xcom_task_push = PythonOperator(
        task_id='xcom_ops_push',
        python_callable=xcoms_push,
    )

    # Task 3: Pull XCom values and do math
    def perform_math_operation(ti):
        num1 = ti.xcom_pull(task_ids='xcom_ops_push', key='num1')
        num2 = ti.xcom_pull(task_ids='xcom_ops_push', key='num2')
        result = num1 * num2
        print(f"The result of the mathematical operation is: {result}")
        return result

    task_math_operation = PythonOperator(
        task_id='perform_math_ops',
        python_callable=perform_math_operation,
    )

    # Task 4: Pull from previous result and multiply
    def xcoms_pull(num3, ti):
        pull_val = ti.xcom_pull(task_ids='perform_math_ops')
        final_result = num3 * pull_val
        print(f"Final multiplication: {num3} * {pull_val} = {final_result}")
        return final_result

    xcom_task_pull = PythonOperator(
        task_id='xcom_ops_pull',
        python_callable=xcoms_pull,
        op_kwargs={'num3': 10},
    )

    # Set dependencies
    task_print_message >> xcom_task_push >> task_math_operation >> xcom_task_pull
