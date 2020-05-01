import os, gzip
import numpy as np
import uuid, datetime
import Classifier as classifier
from MessageBroker import MessageBroker

message_broker = MessageBroker(type="google")

def classify(message):

    #Classifies
    print(message)
    #Unpack the message
    #Classify the image

    class_category = classifier.classify_image(message)
    new_uuid = str(uuid.uuid1())
    new_time = str(datetime.now())
    publisher = message_broker.generate_producer(type='google')
    message = bytes(class_category)

    message_broker.publish_message(type='google', producer_instance=publisher, topic_name='classes', value=message)

    print(message.data)


#Listening to images to classify
consumer = message_broker.generate_consumer(type='google',topic_name='images',gcallback=classify)

try:
   consumer.result()
except KeyboardInterrupt:
    consumer.cancel()

