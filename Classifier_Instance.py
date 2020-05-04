import os, gzip
import numpy as np
import uuid, datetime
import Classifier as classifier
from MessageBroker import MessageBroker

message_broker = MessageBroker(type="google")

def classify(message):

    #Classifies
    img = np.frombuffer(message.data, dtype='uint8')
    class_category = classifier.classify_image(img)
    print(class_category)
    publisher = message_broker.generate_producer(type='google')
    tosend = bytes([class_category])
    message_broker.publish_message(type='google', producer_instance=publisher, topic_name='classes', value=tosend)
    message.ack()


#Listening to images to classify
future = message_broker.generate_consumer(type='google',topic_name='images',gcallback=classify)

try:
   future.result()
except KeyboardInterrupt:
    future.cancel()

