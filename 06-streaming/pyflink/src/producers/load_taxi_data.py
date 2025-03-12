import csv
import json
from kafka import KafkaProducer
from time import time

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    fields_to_keep = [
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount'
    ]

    csv_file = 'data/green_tripdata_2019-10.csv'  # change to your CSV file path if needed
    # row_number = 0
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)


        for row in reader:
            filtered_row = {key: row[key] for key in fields_to_keep if key in row}
            # row_number += 1
            # if row_number > 100:
            #     break
            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green-data"
            # print(f"Row: {row}")
            # print(f"filtered_row: {filtered_row}")
            producer.send('green-trips', value=filtered_row)
    #
    # # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()


if __name__ == "__main__":
    t0 = time()
    main()
    t1 = time()
    took = t1 - t0
    print(f"took: {took}")
