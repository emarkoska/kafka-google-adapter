from MessageBroker import MessageBroker

if __name__ == '__main__':

    running = True
    consumer = MessageBroker.generate_consumer(type = 'kafka', topic_name = 'test', auto_offset_reset='earliest',
                                               bootstrap_servers='localhost:9092', api_version=10)

    try:
        consumer.subscrive("test")

        while running:
            msg = consumer.poll(timeout_ms=10000000)
            if msg is None: continue

            for val in iter(msg.values()):
                print(val[0].value)
    finally:
        consumer.close()

    #Message publishing
    #producer = connector.connect_kafka_producer()
    #connector.publish_message(producer, "test", 'parsed', "automatedyay!")



