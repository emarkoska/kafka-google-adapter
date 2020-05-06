import numpy as np
import sys
import Classifier as classifier
from MessageBroker import MessageBroker
from keras import backend as K
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import tensorflow as tf


message_broker = None

def classify_google(message):
    img = np.frombuffer(message.data, dtype='uint8')
    class_category = classifier.classify_image(img)

    print(class_category)
    publisher = message_broker.generate_producer(type='google')
    tosend = bytes([class_category])
    message_broker.publish_message(type='google', producer_instance=publisher, topic_name='classes', value=tosend)
    message.ack()

def classify_kafka(message):
    img = np.frombuffer(message, dtype='uint8')
    if(img.size == 784): #There are some test messages that aren't actual images.
        class_category = classifier.classify_image(img)
        print(class_category)
        publisher = message_broker.generate_producer(type='kafka')
        tosend = bytes([class_category])
        message_broker.publish_message(producer_instance=publisher, topic_name="classes", key='parsed', value=tosend, type='kafka')


if __name__ == '__main__':
    #specify arks for kafka or google
    # Listening to images to classify
    type = sys.argv[1]
    message_broker = MessageBroker(type = type)

    if type == "google":
        future = message_broker.generate_consumer(type='google', topic_name='images', gcallback=classify_google)
        try:
            future.result()
        except KeyboardInterrupt:
            future.cancel()

    if type == "kafka":
        running = True
        consumer = message_broker.generate_consumer(type='kafka', topic_name='images', auto_offset_reset='earliest',
                                                    bootstrap_servers='localhost:9092', api_version=10)
        try:
            consumer.subscribe("images")
            while running:
                msg = consumer.poll(timeout_ms=10000000)
                if msg is None: continue

                for val in iter(msg.values()):
                    classify_kafka(val[0].value)
        finally:
            consumer.close()