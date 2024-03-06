import datetime
import json

import pandas as pd
import mysql.connector

from confluent_kafka import Consumer, KafkaError
from sqlalchemy import create_engine

def write_data_mysql(final_data):
    host = 'localhost'
    user = 'root'
    password = '1'
    database = 'project'
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
    ten_bang = 'event'
    final_data.to_sql(name=ten_bang, con=engine, if_exists='append', index=False)

consumer  = Consumer({
    'bootstrap.servers':'localhost',
    'group.id': 'project',
    'enable.auto.commit': 'false',
    'auto.offset.reset': 'latest'
})

consumer.subscribe(['project'])

def main():
    while True:
        msg = consumer.poll()  
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event, not an error
                continue
            else:
                print(msg.error())
                break

        # Process the message
        try:
            # Decode the value and convert it to a Python object
            value_str = msg.value().decode('utf-8')
            value_dict = json.loads(value_str)

            print(value_dict)
        except Exception as e:
            print(f"Error processing message: {e}")

main()