from kafka import KafkaConsumer, KafkaProducer
from google.cloud import pubsub_v1
import os

class KafkaConnector():

    def publish_message(self, producer_instance, topic_name, key, value):
        try:
            key_bytes = bytes(key, encoding='utf-8')
            value_bytes = bytes(value, encoding='utf-8')
            producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
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
            _producer = publisher = pubsub_v1.PublisherClient()
        except Exception as ex:
            print('Exception while connecting Google Pub/Sub')
            print(str(ex))
        finally:
            return _producer

    def connect_google_consumer(self, topic_name, subscription_name):
        _consumer = None
        try:
            _consumer = pubsub_v1.SubscriberClient()
            topic_name = 'projects/{project_id}/topics/{topic}'.format(
                project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
                topic=topic_name,  # Set this to something appropriate.
            )
            subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
                project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
                sub=subscription_name,  # Set this to something appropriate.
            )
            _consumer.create_subscription(name=subscription_name, topic=topic_name)

        except Exception as ex:
            print("Exception while instantiating consumer")
            print(str(ex))
        finally:
            return _consumer

    def publish_message(self, producer_instance, topic_name, message):
        try:
            producer_instance.publish(topic_name, message, spam='eggs')
            print('Message published successfully.')
        except Exception as ex:
            print('Exception in publishing message')
            print(str(ex))

class MessageBroker():
    kafka = None
    google = None

    def __init__(self, type):
        if type == "kafka": self.kafka = KafkaConnector()
        if type == "google": self.google = GooglePSConnector()

    def publish_message(self, type, producer_instance, topic_name, key, value):
        if type == "kafka":
            self.kafka.publish_message(producer_instance=producer_instance, topic_name=topic_name, key=key, value=value)
        if type == "google":
            self.google.publish_message(producer_instance=producer_instance,topic_name=topic_name,message=value)

    def generate_consumer(self, type, topic_name, auto_offset_reset, bootstrap_servers, api_version, subscription_name):
        if type == "kafka":
            return self.kafka.connect_kafka_consumer(topic_name=topic_name, auto_offset_reset=auto_offset_reset,
                                              bootstrap_servers=bootstrap_servers, api_version = api_version,
                                              consumer_timeout_ms=100000)
        if type == "google":
            return self.google.connect_google_consumer(topic_name=topic_name,subscription_name=subscription_name)

    def generate_producer(self, type):
        if type == "kafka":
            return self.kafka.connect_kafka_producer()
        if type == "google":
            return self.google.connect_google_producer()