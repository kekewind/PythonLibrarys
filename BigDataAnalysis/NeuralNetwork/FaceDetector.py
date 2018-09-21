import math
import os
import pickle
import re

import cv2
import dlib
import face_recognition
import numpy
from pkg_resources import resource_filename
from sklearn import neighbors


class FaceRecognition:
    def __init__(self, knn_module_path=None):
        self.open_cv_module = resource_filename(__name__, "modules/haarcascade_frontalface_default.xml")
        self.cnn_module = resource_filename(__name__, "modules/mmod_human_face_detector.dat")
        self.predictor_module = resource_filename(__name__, "modules/shape_predictor_68_face_landmarks.dat")
        self.knn_module_path = knn_module_path if knn_module_path else \
            resource_filename(__name__, "modules/shape_predictor_68_face_landmarks.dat")
        self.ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}

    def face_detector(self, image_path):
        face_patterns = cv2.CascadeClassifier(self.open_cv_module)
        sample_image = cv2.cv2.imdecode(numpy.fromfile(image_path, dtype=numpy.uint8), -1)
        faces = face_patterns.detectMultiScale(sample_image, scaleFactor=1.1, minNeighbors=5, minSize=(100, 100))
        predictor = dlib.shape_predictor(self.predictor_module)
        if len(faces) < 1:
            is_located = False
            detector = dlib.cnn_face_detection_model_v1(self.cnn_module)
            frames = [frame.rect for frame in detector(sample_image, 0)]
        else:
            is_located = True
            detector = dlib.get_frontal_face_detector()
            gray = cv2.cvtColor(sample_image, cv2.COLOR_BGR2GRAY)
            frames = detector(gray, 1)
        for (i, rect) in enumerate(frames):
            shape = predictor(sample_image, rect)
            features = numpy.zeros((68, 2), dtype='int')
            for each in range(0, 68):
                features[each] = (shape.part(each).x, shape.part(each).y)
            for (x, y) in features:
                cv2.circle(sample_image, (x, y), 2, (0, 0, 255), -1)
        if is_located:
            for (x, y, w, h) in faces:
                cv2.rectangle(sample_image, (x, y), (x + w, y + h), (0, 255, 0), 2)
        else:
            for (i, rect) in enumerate(frames):
                minx = rect.left()
                miny = rect.top()
                w = rect.right() - minx
                h = rect.bottom() - miny
                cv2.rectangle(sample_image, (minx, miny), (minx + w, miny + h), (0, 255, 0), 2)
        cv2.cv2.imencode('.jpg', sample_image)[1].tofile(os.path.dirname(image_path) + "\\output.jpg")

    def knn_train(self, train_dir, model_save_path=None, n_neighbors=None, knn_algo='ball_tree', verbose=False):
        """
        Trains a k-nearest neighbors classifier for face recognition.
        :param train_dir: directory that contains a sub-directory for each known person, with its name.
         (View in source code to see train_dir example tree structure)
         Structure:
            <train_dir>/
            ├── <person1>/
            │   ├── <somename1>.jpeg
            │   ├── <somename2>.jpeg
            │   ├── ...
            └── ...
        :param model_save_path: (optional) path to save model on disk
        :param n_neighbors: (optional) number of neighbors to weigh in classification. Chosen automatically if not specified
        :param knn_algo: (optional) underlying data structure to support knn.default is ball_tree
        :param verbose: verbosity of training
        :return: returns knn classifier that was trained on the given data.
        """
        x = []
        y = []
        # Loop through each person in the training set
        for class_dir in os.listdir(train_dir):
            if not os.path.isdir(os.path.join(train_dir, class_dir)):
                continue

            # Loop through each training image for the current person
            for img_path in self.image_files_in_folder(os.path.join(train_dir, class_dir)):
                image = face_recognition.load_image_file(img_path)
                face_bounding_boxes = face_recognition.face_locations(image)

                if len(face_bounding_boxes) != 1:
                    # If there are no people (or too many people) in a training image, skip the image.
                    if verbose:
                        print("Image {} not suitable for training: {}".format(img_path, "Didn't find a face" if len(
                            face_bounding_boxes) < 1 else "Found more than one face"))
                else:
                    # Add face encoding for current image to the training set
                    x.append(face_recognition.face_encodings(image, known_face_locations=face_bounding_boxes)[0])
                    y.append(class_dir)

        # Determine how many neighbors to use for weighting in the KNN classifier
        if n_neighbors is None:
            n_neighbors = int(round(math.sqrt(len(x))))
            if verbose:
                print("Chose n_neighbors automatically:", n_neighbors)

        # Create and train the KNN classifier
        knn_clf = neighbors.KNeighborsClassifier(n_neighbors=n_neighbors, algorithm=knn_algo, weights='distance')
        knn_clf.fit(x, y)

        # Save the trained KNN classifier
        if model_save_path is None:
            model_save_path = self.knn_module_path
        with open(model_save_path, 'wb') as f:
            pickle.dump(knn_clf, f)

        return knn_clf

    def __knn_predict(self, x_img_path, knn_clf=None, distance_threshold=0.6):
        """
        Recognizes faces in given image using a trained KNN classifier
        :param x_img_path: path to image to be recognized
        :param knn_clf: (optional) a knn classifier object. if not specified, model_save_path must be specified.
        :param distance_threshold: (optional) distance threshold for face classification. the larger it is,the more chance
               of mis-classifying an unknown person as a known one.
        :return: a list of names and face locations for the recognized faces in the image: [(name, bounding box), ...].
            For faces of unrecognized persons, the name 'unknown' will be returned.
        """
        if not os.path.isfile(x_img_path) or os.path.splitext(x_img_path)[1][1:] not in self.ALLOWED_EXTENSIONS:
            raise Exception("Invalid image path: {}".format(x_img_path))
        if knn_clf is None and self.knn_module_path is None:
            raise Exception("Must supply knn classifier either through knn_clf or model_path")
        # Load a trained KNN model (if one was passed in)
        if knn_clf is None:
            with open(self.knn_module_path, 'rb') as f:
                knn_clf = pickle.load(f)
        # Load image file and find face locations
        x_img = face_recognition.load_image_file(x_img_path)
        x_face_locations = face_recognition.face_locations(x_img)
        # If no faces are found in the image, return an empty result.
        if len(x_face_locations) == 0:
            x_face_locations = face_recognition.face_locations(x_img, number_of_times_to_upsample=0, model="cnn")
        if len(x_face_locations) == 0:
            return []
        # Find encodings for faces in the test image
        faces_encodings = face_recognition.face_encodings(x_img, known_face_locations=x_face_locations)
        # Use the KNN model to find the best matches for the test face
        closest_distances = knn_clf.kneighbors(faces_encodings, n_neighbors=1)
        are_matches = [closest_distances[0][i][0] <= distance_threshold for i in range(len(x_face_locations))]
        # Predict classes and remove classifications that aren't within the threshold
        return [(predict, loc) if rec else ("unknown", loc) for predict, loc, rec in
                zip(knn_clf.predict(faces_encodings), x_face_locations, are_matches)]

    def image_recognition(self, text_image_path):
        if os.path.isfile(text_image_path):
            predictions = self.__knn_predict(text_image_path)
            identify_dict = []
            for name, (top, right, bottom, left) in predictions:
                identify_dict.append({
                    'name': name,
                    'pos': [top, right, bottom, left]
                })
            return identify_dict
        else:
            return []

    @staticmethod
    def image_files_in_folder(folder):
        return [os.path.join(folder, f) for f in os.listdir(folder) if re.match(r'.*\.(jpg|jpeg|png)', f, flags=re.I)]
