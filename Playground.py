import os
from google.cloud import pubsub_v1
import numpy as np
from Classifier import load_mnist, classify_image

# A module just for random testing


str = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00'
sdf = int.from_bytes(str, byteorder='big')
print(sdf)