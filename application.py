from MessageBroker import MessageBroker
from Classifier import load_mnist
import time, sys

#Module with application instance.
#The module intends to listen to messages from either google or kafka.
#Once a message is received, the classifier is called

def classify_images(type):
    X_test, y_test = load_mnist('Data/', kind='t10k')
    if type == "google":
        message_broker = MessageBroker("google")

        # Listening to print out classes
        consumer = message_broker.generate_consumer(type='google', topic_name='classes', gcallback=callback)

        # Sending images
        publisher = message_broker.generate_producer(type='google')
        for img in X_test:
            print("Sending an image.")
            message = img.tobytes()
            message_broker.publish_message(type='google', producer_instance=publisher, topic_name='images', value=message)
            time.sleep(10)

        try:
           consumer.result()
        except KeyboardInterrupt:
           consumer.cancel()

    if type == 'kafka':
        # Sending images
        message_broker = MessageBroker(type='kafka')
        for img in X_test:
            print("Sending an image.")
            producer = message_broker.generate_producer(type="kafka")
            message_broker.publish_message(producer_instance=producer, topic_name="images", key='parsed',
                                           value=img.tobytes(), type='kafka')
            time.sleep(10)


        # running = True
        # message_broker = MessageBroker(type='kafka')
        # consumer = message_broker.generate_consumer(type='kafka', topic_name='classes', auto_offset_reset='earliest',
        #                                             bootstrap_servers='localhost:9092', api_version=10)
        # try:
        #     consumer.subscribe("classes")
        #     while running:
        #         msg = consumer.poll(timeout_ms=10000000)
        #         if msg is None: continue
        #
        #         for val in iter(msg.values()):
        #             print(val[0].value)
        # finally:
        #     consumer.close()




def kafka_testtopic():
    #Tests run on a test topic for simple communication testing of the adapter

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
    #Basic callback
    c = int.from_bytes(message.data, byteorder='big')
    print("Image class is %s" % c)
    message.ack

def google_testtopic():
    # Tests run on a test topic for simple communication testing of the adapter

    #Consuming messages
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
    type = sys.argv[1]

    if type == "google":
        classify_images("google")
    if type == "kafka":
        classify_images("kafka")
    #kafka_testtopic()