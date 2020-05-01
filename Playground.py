import os
from google.cloud import pubsub_v1

# A module just for random testing


publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/fashionmnistclassifier/topics/test'.format(
    project_id=os.getenv('FashionMNISTClassifier'),
    topic='test',  # Set this to something appropriate.
)
publisher.publish(topic_name, b'My first message!', spam='eggs')