# !/usr/bin/python
# -*- coding: utf-8 -*-


import time

import numpy as np
import tensorflow as tf
from PIL import Image
from PIL import ImageDraw


class ObjectDetector:
    def __init__(self):
        self.detection_graph = self._build_graph()
        self.sess = tf.Session(graph=self.detection_graph)

    @staticmethod
    def _build_graph():
        detection_graph = tf.Graph()
        with detection_graph.as_default():
            od_graph_def = tf.GraphDef()
            with tf.gfile.GFile('frozen_inference_graph.pb', 'rb') as fid:
                serialized_graph = fid.read()
                od_graph_def.ParseFromString(serialized_graph)
                tf.import_graph_def(od_graph_def, name='')

        return detection_graph

    def _load_image_into_numpy_array(self, image):
        (im_width, im_height) = image.size
        return np.array(image.getdata()).reshape(
            (im_height, im_width, 3)).astype(np.uint8)

    def detect(self, image):
        image_np = self._load_image_into_numpy_array(image)
        image_np_expanded = np.expand_dims(image_np, axis=0)
        image_tensor = self.detection_graph.get_tensor_by_name('image_tensor:0')
        boxes = self.detection_graph.get_tensor_by_name('detection_boxes:0')
        scores = self.detection_graph.get_tensor_by_name('detection_scores:0')
        classes = self.detection_graph.get_tensor_by_name('detection_classes:0')
        boxes, scores, classes = self.sess.run(
            [boxes, scores, classes],
            feed_dict={image_tensor: image_np_expanded})
        boxes, scores, classes = map(np.squeeze, [boxes, scores, classes])
        return boxes, scores, classes.astype(int)


def draw_bounding_box_on_image(image, box, color='red', thickness=4):
    draw = ImageDraw.Draw(image)
    im_width, im_height = image.size
    ymin, xmin, ymax, xmax = box
    (left, right, top, bottom) = (xmin * im_width, xmax * im_width,
                                  ymin * im_height, ymax * im_height)
    draw.line([(left, top), (left, bottom), (right, bottom),
               (right, top), (left, top)], width=thickness, fill=color)


def save_image(image, n):
    image.save("a" + str(n) + "b.png")


def detect_objects(image_path):
    image = Image.open(image_path).convert('RGB')
    boxes, scores, classes = client.detect(image)
    image.thumbnail((480, 480), Image.ANTIALIAS)

    new_images = {}
    for box, score, obj_class in zip(boxes, scores, classes):
        if score < 0.7:
            continue
        if obj_class not in new_images.keys():
            new_images[obj_class] = image.copy()
        draw_bounding_box_on_image(new_images[obj_class], box,
                                   thickness=int(score * 10) - 4)
    for obj_class, new_image in new_images.items():
        print(obj_class)
        save_image(new_image, obj_class)

    return result


client = ObjectDetector()
a = time.time()
result = detect_objects('a.jpg')
b = time.time()
print(b - a)
