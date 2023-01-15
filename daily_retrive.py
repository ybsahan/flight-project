from datetime import datetime, timedelta
import json
import os
import requests
from airflow.operators.python_operator import PythonOperator
from azure.storage.blob import BlobServiceClient

from airflow import DAG
from airflow.models import Variable

# Define default_args dictionary to pass to DAG
default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 1, 9, 0, 0),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def get_flight_data(airport: str, **kwargs):
    execution_time = kwargs['data_interval_start']
    print(f"Running task 1 at {execution_time}")
    execution_time_str = execution_time.strftime('%Y-%m-%d %H:%M:%S')
    dt_object = datetime.strptime(execution_time_str, '%Y-%m-%d %H:%M:%S')
    year = dt_object.year
    month = dt_object.month
    day = dt_object.day
    hour = dt_object.hour
    print(f"Year: {year}, Month: {month}, Day: {day}, Hour: {hour}")

    appid = Variable.get("APP_ID")
    appkey = Variable.get("APP_KEY")

    response = requests.get(
        f'https://api.flightstats.com/flex/flightstatus/rest/v2/json/airport/status/{airport}/arr/{year}/{month}/{day}/{hour}',
        params={'appId': appid, 'appKey': appkey})

    # Check the status code of the response
    if response.status_code != 200:
        print(f'Error: {response.status_code}')
    else:
        # Get the arrival flights from the response
        arrivals = response.json()["flightStatuses"]
        a = []
        for i in arrivals:
            if i["status"] == "L":
                a.append(i)

        print(f"Number of arrival flights at {airport} on {year}-{month}-{day} at {hour} o'clock: {len(a)}")

        kwargs['ti'].xcom_push(key=f"flight-data-{year}-{month}-{day}-{hour}", value=a)

        return a


def insert_data_into_bs( blob_name: str, container_name: str, **kwargs) -> object:
    execution_time = kwargs['data_interval_start']
    print(f"Running task 1 at {execution_time}")
    execution_time_str = execution_time.strftime('%Y-%m-%d %H:%M:%S')
    dt_object = datetime.strptime(execution_time_str, '%Y-%m-%d %H:%M:%S')
    year = dt_object.year
    month = dt_object.month
    day = dt_object.day
    hour = dt_object.hour

    data = kwargs['ti'].xcom_pull(key=f"flight-data-{year}-{month}-{day}-{hour}", task_ids='get_flightstats_data')

    # Azure Blob Storage için BlobServisClient nesnesi oluşturun
    service = BlobServiceClient.from_connection_string(Variable.get("BLOB_CONNECTION"))

    # Depolama kapsayıcısı oluşturun veya mevcut bir kapsayıcı seçin
    container = service.get_container_client(container_name)

    # Dosya yüklemek için BlobClient nesnesi oluşturun
    blob = container.get_blob_client(blob_name + f"-{year}-{month}-{day}-{hour}.json")

    blob.upload_blob(json.dumps(data), overwrite=True)


# Create a DAG instance
dag = DAG(
    'hourly_retrieve_flight_data',
    default_args=default_args,
    schedule_interval="0 * * * *",
)

# Create an instance of PythonOperator for task1
get_flightstats_data = PythonOperator(
    task_id='get_flightstats_data',
    provide_context=True,
    python_callable=get_flight_data,
    op_kwargs={'airport': 'IST'},
    dag=dag,
)

insert_blob_storage = PythonOperator(
    task_id='insert_blob_storage',
    provide_context=True,
    python_callable=insert_data_into_bs,
    op_kwargs={'blob_name': 'flight-stats', 'container_name': 'flight-container'},
    dag=dag,
)

get_flightstats_data >> insert_blob_storage
