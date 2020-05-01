import os
import gzip
import numpy as np
from keras.utils.np_utils import to_categorical
from sklearn.model_selection import train_test_split
from keras.models import Sequential, load_model
from keras.layers import Dense, Conv2D, Activation, MaxPool2D, Flatten, Dropout, BatchNormalization
from keras.preprocessing.image import ImageDataGenerator

"""
This module defines a classifier for the MNIST Fashion dataset.

The module allows for loading the dataset, training a model,
and classifying the images from a predefined test set.
"""


def load_mnist(path, kind):
    """
    Loads the data from path, relying on the name of the
    initial MNIST Fashion dataset files

    Args:
        path: path to dataset
        kind: 'train' for training set or 't10k' for test set

    Returns: arrays of images and their labels
    """
    labels_path = os.path.join(path,
                               '%s-labels-idx1-ubyte.gz'
                               % kind)
    images_path = os.path.join(path,
                               '%s-images-idx3-ubyte.gz'
                               % kind)

    with gzip.open(labels_path, 'rb') as lbpath:
        labels = np.frombuffer(lbpath.read(), dtype=np.uint8,
                               offset=8)

    with gzip.open(images_path, 'rb') as imgpath:
        images = np.frombuffer(imgpath.read(), dtype=np.uint8,
                               offset=16).reshape(len(labels), 784)

    return images, labels

def train_model(path):
    """
        Trains an image classifier using convolutional neural networks
        for the Fashion MNIST dataset.
        Accuracy on validation set (subset of training set) ~94%.

        Args:
            path: path to dataset

        Returns: Void; Model is saved in a predefined file and later on loaded.
    """

    X_train, y_train = load_mnist(path, kind='train')
    X_train = X_train / 255.0
    X_train = X_train.reshape(-1, 28, 28, 1)
    y_train = to_categorical(y_train, num_classes=10)
    x_train, x_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.1, random_state=2)

    model = Sequential()

    model.add(Conv2D(filters=32, kernel_size=(3, 3), padding='Same', input_shape=(28, 28, 1)))
    model.add(BatchNormalization())
    model.add(Activation("relu"))
    model.add(MaxPool2D(pool_size=(2, 2)))

    model.add(Conv2D(filters=64, kernel_size=(3, 3), padding='Same'))
    model.add(BatchNormalization())
    model.add(Activation("relu"))
    model.add(MaxPool2D(pool_size=(2, 2)))

    model.add(Flatten())
    model.add(Dense(256))
    model.add(BatchNormalization())
    model.add(Activation("relu"))
    model.add(Dropout(0.25))

    model.add(Dense(10, activation='softmax'))

    model.compile(optimizer="adam", loss="categorical_crossentropy", metrics=["accuracy"])
    epochs = 50
    batch_size = 1000

    datagen = ImageDataGenerator(
        featurewise_center=False,  # set input mean to 0 over the dataset
        samplewise_center=False,  # set each sample mean to 0
        featurewise_std_normalization=False,  # divide inputs by std of the dataset
        samplewise_std_normalization=False,  # divide each input by its std
        zca_whitening=False,  # dimesion reduction
        rotation_range=0.1,  # randomly rotate images in the range
        zoom_range=0.1,  # Randomly zoom image
        width_shift_range=0.1,  # randomly shift images horizontally
        height_shift_range=0.1,  # randomly shift images vertically
        horizontal_flip=False,  # randomly flip images
        vertical_flip=False)  # randomly flip images

    datagen.fit(x_train)

    history = model.fit_generator(datagen.flow(x_train, y_train, batch_size=batch_size),
                                  shuffle=True, epochs=epochs, validation_data=(x_val, y_val),
                                  verbose=2, steps_per_epoch=x_train.shape[0] // batch_size)

    model.save('model_saved.h5')

def classify_image(image):
    """
        Classifies a single input image fed from the predefined MNIST Fashion
        training set.

        Args:
            image: image to be classified

            Returns: integer containing the class of the input image
    """

    model = load_model('model_saved.h5')
    test_img = image / 255
    test_img = test_img.reshape(-1,28,28,1)
    test_img = test_img.reshape(1, 28, 28, 1)
    preds = model.predict_classes(test_img)
    return preds[0]

def classify_images():
    X_test, y_test = load_mnist('Data/', kind='t10k')
    #X_test = X_test / 255.0
    #X_test = X_test.reshape(-1, 28, 28, 1)

    for img in X_test:
        print("The predicted class is %s" % classify_image(img))

if __name__ == '__main__':
    classify_images()
    #train_model('Data/')