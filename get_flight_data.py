import requests, os


# appId = '6dee5e2b'
# appKey = 'f45711fcf0e64053197b464a3f7efb37'
#
# # Set the parameters for the API request
# airport = 'IST'  # The IATA code for Istanbul Airport
# year = 2023  # The year
# month = 1  # The month (1-12)
# day = 1  # The day of the month (1-31)
# hour = 7  # The hour of the day


def get_flight_data(airport: str, year: int, month: int, day: int, hour: int):
    # appid = '6dee5e2b'
    # appkey = 'f45711fcf0e64053197b464a3f7efb37'
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

