from kafka import KafkaConsumer, KafkaProducer
from google.cloud import pubsub_v1
import os

class MessageBroker():
    kafka = None
    google = None

    def __init__(self, type):
        if type == "kafka": self.kafka = KafkaConnector()
        if type == "google": self.google = GooglePSConnector()

    def publish_message(self, type, topic_name, value, producer_instance = None, key = None):
        if type == "kafka":
            self.kafka.kafka_publish_message(producer_instance=producer_instance, topic_name=topic_name, key=key, value=value)
        if type == "google":
            self.google.google_publish_message(producer_instance=producer_instance,topic_name=topic_name,message=value)

    def generate_consumer(self, type, topic_name, auto_offset_reset = None, bootstrap_servers = None, api_version = None, gcallback = None):
        if type == "kafka":
            return self.kafka.connect_kafka_consumer(topic_name=topic_name, auto_offset_reset=auto_offset_reset,
                                              bootstrap_servers=bootstrap_servers, api_version = api_version,
                                              consumer_timeout_ms=100000)
        if type == "google":
            return self.google.connect_google_consumer(topic_name=topic_name,gcallback = gcallback)

    def generate_producer(self, type):
        if type == "kafka":
            return self.kafka.connect_kafka_producer()
        if type == "google":
            return self.google.connect_google_producer()

class KafkaConnector():

    def kafka_publish_message(self, producer_instance, topic_name, key, value):
        try:
            key_bytes = bytes(key, encoding='utf-8')
            producer_instance.send(topic_name, key=key_bytes, value=value)
            producer_instance.flush()
            print('Message published successfully.')
        except Exception as ex:
            print('Exception in publishing message')
            print(str(ex))

    def connect_kafka_consumer(self, topic_name, auto_offset_reset, bootstrap_servers,
                               api_version, consumer_timeout_ms):
        _consumer = None
        try:
            _consumer = KafkaConsumer(topic_name, auto_offset_reset=auto_offset_reset,
                                      bootstrap_servers=[bootstrap_servers], api_version=(0, api_version),
                                      consumer_timeout_ms=consumer_timeout_ms)
        except Exception as ex:
            print("Exception while instantiating consumer")
            print(str(ex))
        finally:
            return _consumer

    def connect_kafka_producer(self):
        _producer = None
        try:
            _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
        except Exception as ex:
            print('Exception while connecting Kafka')
            print(str(ex))
        finally:
            return _producer

class GooglePSConnector():

    def connect_google_producer(self):
        _producer = None
        try:
            _producer  = pubsub_v1.PublisherClient()
        except Exception as ex:
            print('Exception while connecting Google Pub/Sub')
            print(str(ex))
        finally:
            return _producer

    def connect_google_consumer(self, topic_name, gcallback):
        _consumer = None
        try:
            _consumer = pubsub_v1.SubscriberClient()
            topic = ('projects/fashionmnistclassifier/topics/%s' % topic_name).format(
                project_id=os.getenv('FashionMNISTClassifier'),
                topic=topic_name,
            )
            subscription_name = ('projects/fashionmnistclassifier/subscriptions/%s_subscription' % topic_name).format(
                project_id=os.getenv('FashionMNISTClassifier'),
                sub= "%s_subscription" % topic_name,
            )

            consumer = _consumer.subscribe(subscription_name, gcallback)
        except Exception as ex:
            print("Exception while instantiating consumer")
            print(str(ex))
        finally:
            return consumer

    def google_publish_message(self, producer_instance, topic_name, message):
        try:
            topic = ('projects/fashionmnistclassifier/topics/%s' % topic_name).format(
                project_id=os.getenv('FashionMNISTClassifier'),
                topic=topic_name,
            )
            producer_instance.publish(topic, message, spam='eggs')
            print('Message published successfully.')
        except Exception as ex:
            print('Exception in publishing message')
            print(str(ex))

