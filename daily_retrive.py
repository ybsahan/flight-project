from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests, os

# Define default_args dictionary to pass to DAG
default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 12, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_flight_data(airport: str, year: int, month: int, day: int, hour: int):
    appid = os.getenv("APP_ID")
    appkey = os.getenv("APP_KEY")

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

        return a


# Create a DAG instance
dag = DAG(
    'daily_retrieve_flight_data',
    default_args=default_args,
    schedule_interval="0 1 * * *",
)


# Create an instance of PythonOperator for task1
task1 = PythonOperator(
    task_id='task1',
    python_callable=get_flight_data,
    op_kwargs={'airport': 'IST', 'year': '',
               'month': '', 'day': '', 'hour': ''},
    dag=dag,
)
task1
