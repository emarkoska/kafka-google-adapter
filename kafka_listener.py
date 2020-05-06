import os
from google.cloud import pubsub_v1
import numpy as np
from MessageBroker import MessageBroker


running = True
message_broker = MessageBroker(type='kafka')
consumer = message_broker.generate_consumer(type='kafka', topic_name='classes', auto_offset_reset='earliest',
                                                    bootstrap_servers='localhost:9092', api_version=10)
try:
    consumer.subscribe("classes")
    while running:
        msg = consumer.poll(timeout_ms=10000000)
        if msg is None: continue

        for val in iter(msg.values()):
            print(int.from_bytes(val[0].value, byteorder='big'))
finally:
    consumer.close()