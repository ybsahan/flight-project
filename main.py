from get_flight_data import get_flight_data
from insert_blob_storage import insert_data_into_bs


def main(hour):
    data = get_flight_data('IST', 2023, 1, 5, hour)
    insert_data_into_bs(data, "flight-blob", "flight-container", hour)


if __name__ == "__main__":
    for i in range(0, 24):
        main(i)
