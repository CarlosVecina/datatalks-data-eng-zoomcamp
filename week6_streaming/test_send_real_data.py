import json
import time

import pandas as pd
from kafka import KafkaProducer

TOPIC_NAME = "green-trips"
MSG_FIELDS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
]


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


server = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=[server], value_serializer=json_serializer)

producer.bootstrap_connected()

df_green = pd.read_csv("data/green_tripdata_2019-10.csv.gz")[MSG_FIELDS]

t0 = time.time()

for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send(TOPIC_NAME, value=row_dict)

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')

producer.flush()

t2 = time.time()
print(f'took {(t2 - t1):.2f} seconds')