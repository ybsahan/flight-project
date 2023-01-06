from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "me",
    "start_date": datetime(2022, 1, 1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    "my_dag_id",
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

# Define a function to be used as the task callable
@task(task_id="print_the_context")
def print_context():
    """Print the Airflow context and ds variable from the context."""
    print("nothing")
    return 'Whatever you return gets printed in the logs'

# Create a PythonOperator task
task = PythonOperator(
    task_id="print_the_context",
    python_callable=print_context,
    provide_context=True,
    dag=dag,
)

# Set up task dependencies
task