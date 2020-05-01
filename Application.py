from MessageBroker import MessageBroker
from Classifier import load_mnist
import uuid
from datetime import datetime

def classify_images():
    X_test, y_test = load_mnist('Data/', kind='t10k')
    message_broker = MessageBroker("google")

    # Sending images
    img = X_test[1]
    new_uuid = str(uuid.uuid1())
    new_time = str(datetime.now())
    publisher = message_broker.generate_producer(type='google')
    message = img
    message_broker.publish_message(type='google', producer_instance=publisher, topic_name='images', value=message)

    #Listening to print out classes
    consumer = message_broker.generate_consumer(type='google',topic_name='classes',gcallback=callback)

    try:
       consumer.result()
    except KeyboardInterrupt:
       consumer.cancel()





def kafka_testtopic():

    #Consume
    running = True
    message_broker = MessageBroker(type='kafka')
    consumer = message_broker.generate_consumer(type='kafka', topic_name='test', auto_offset_reset='earliest',
                                               bootstrap_servers='localhost:9092', api_version=10)
    try:
        consumer.subscribe("test")
        while running:
            msg = consumer.poll(timeout_ms=10000000)
            if msg is None: continue

            for val in iter(msg.values()):
                print(val[0].value)
    finally:
        consumer.close()

    # Message publishing
    # producer = connector.connect_kafka_producer()
    # connector.publish_message(producer, "test", 'parsed', "automatedyay!")

def callback(message):
    print(message.data)

def google_testtopic():
    #Consume
    message_broker = MessageBroker(type='google')
    #consumer = message_broker.generate_consumer(type='google',topic_name='test',gcallback=callback)
    #try:
    #    consumer.result()
    #except KeyboardInterrupt:
    #    consumer.cancel()

    #Message publishing
    publisher = message_broker.generate_producer(type='google')
    message_broker.publish_message(type='google',producer_instance=publisher,topic_name='test',value=b"test?")



if __name__ == '__main__':
    classify_images()



