from PIL import Image
import pickle
import numpy
import threading
import os
import uuid


class SamplesLoad:
    def __init__(self, path):
        self.path = path
        self.sample_block_count = 0
        self.sample_width = 32
        self.sample_height = 32
        self.sample_size = self.sample_width * self.sample_height
        self.sample_chanel = 3
        self.sample_class = [""]
        self.sample_block_size = self.sample_chanel * self.sample_size
        self.data_dic = []

    def image_samples_load(self, path):
        for each in os.listdir(self.path):
            if each.__contains__('data_batch'):
                with open(self.path + each, "rb") as f:
                    self.data_dic.append(pickle.load(f, encoding="latin1"))
        for each in self.data_dic:
            LT = self.LoadThread(threading.Condition(), uuid.uuid4(), self, each, path)
            LT.start()

    def image_gene(self, data, path):
        large = Image.new("RGB", (3200, 3200))
        for index_ver in range(0, 100):
            for index in range(0, 100):
                image_label = data['filenames'][index + index_ver * 100]
                image = numpy.reshape(data['data'][index + index_ver * 100],
                                      (self.sample_chanel, self.sample_width, self.sample_height))
                chanel = []
                for each in range(self.sample_chanel):
                    chanel.append(Image.fromarray(image[each]))
                img = Image.merge("RGB", tuple(chanel))
                box = (index * 32, index_ver * 32, (index + 1) * 32, (index_ver + 1) * 32)
                large.paste(im=img, box=box)
                del img, image, image_label
        large.save(path + str(uuid.uuid4()) + ".png")

    class LoadThread(threading.Thread):
        out = None

        def __init__(self, cond, name, out, data, path):
            self.out = out
            super(out.LoadThread, self).__init__()
            self.cond = cond
            self.name = name
            self.data = data
            self.path = path

        def run(self):
            self.cond.acquire()
            self.out.image_gene(self.data, self.path)
            self.cond.release()


s = SamplesLoad("E:\\Python_Pycharm_Workspace\\BookNetWorm\\NeuralNetwork\\cifar_10_batchs_py\\")
s.image_samples_load("E:\\Python_Pycharm_Workspace\\BookNetWorm\\NeuralNetwork\\cifar_10_batchs_py\\image\\")
