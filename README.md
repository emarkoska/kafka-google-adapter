# kafka-google-adapter
An adapter interfacing between google pub/sub and kafka for an image classification application.
The gist of the functionality lies in Classifier.py and MessageBroker.py.adapter.
The system assumes an installation of kafka (locally) and a project set up with Google pub/sub.

Both streaming services contain the following topics: images, classes, test

# Running
The project demonstrates communication between Classifier_instance -> MessageBroker - kafka and Classifier_instance -> MessageBroker -> google

To run the system with kafka:
* Start the kafka local instance
* Open cmd. Navigate to project directory. Run 'classifier_instance.py kafka'
* Open cmd. Navigate to project directory. Run 'application.py kafka'
* Open cmd. Navigate to project directory. Run 'kafka_listener.py'


To run the system with google:
* Open cmd. Navigate to the directory of the project. Run 'classifier_instance google'
* Open cmd. Navigate to the directory of the project. Run 'application.py google'

It's important that all messages are ACK-ed before testing either kafka or google.

# Classification
Classifier.py contains functions for loading the training set, training a model, and storing it. It also contains functions for classifying individual images.Classifier

# Usage of MessageBroker

Classifier_instance.py calls functions from Classifier.py. It asynchronously subscribes to the topic images using MessageBroker
Once it receives an image, it calls a classification function. The class is the printed and posted as a message to the classes topic.

Application.py has a kafka mode and a google mode.
* In a google mode, it asynchronously subscribes to the classes topic.
Then, in a loop, it sends images from the FashionMNIST dataset to the images topic on google pub/sub.
As the classifier_instance is subscribed to this topic, it will post a message to the classes topic, to which Application.py is subscribed asynchronously.
* In kafka mode, Application.py subscribes to the images topic and in a loop sends them to the images topic.
As the classifier_instance is subscribed to the images topic, it posts the class of the image to the classes topic.
A separate script - kafka_listener.py subscribes to the classes topic in kafka and prints out the class of the image.
