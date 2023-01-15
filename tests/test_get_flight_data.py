import unittest
from unittest.mock import patch
import datetime
from get_flight_data import get_flight_data


class TestGetFlightData(unittest.TestCase):

    @patch('requests.get')
    def test_get_flight_data(self, mock_get):
        # Set up mock response
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "flightStatuses": [
                {"status": "L"}
            ]
        }

        # Set up test data
        airport = "IST"
        year = 2023
        month = 1
        day = 15
        hour = 1

        # Call the function
        result = get_flight_data(airport, year, month, day, hour)

        # Assert that the correct number of flights with status "L" were returned
        self.assertEqual(len(result), 1)

        self.assertEqual(result, [{"status": "L"}])
