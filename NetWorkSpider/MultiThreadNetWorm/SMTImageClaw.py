# -*- coding:UTF-8 -*-
import json
import os
import re
import shutil
import sys
import threading
import time
import uuid

import redis
from bs4 import BeautifulSoup
from selenium.webdriver.edge import webdriver

from BasicLibrarys.Common import HttpRequestBase
from BigDataAnalysis.NeuralNetwork.FaceDetector import FaceRecognition


class ImageClaw:
    def __init__(self, task_info):
        self.task_info = task_info
        self.nums = 0
        self.image_url_queue = []
        self.backup_queue = []
        self.__threads = {}
        self.request_image = "http://image.baidu.com/search/acjson?tn=resultjson_com&ipn=rj&fp=result&word={0}&pn={1}&rn=30"
        self.server = "http://image.baidu.com/search/index?tn=baiduimage&word={0}"
        self.chrome_driver = "D:\\Software\\Python3.7.3\\selenium\\webdriver\\chromedriver.exe"
        self.edge_driver = "D:\Software\Python3.7.3\selenium\webdriver\MicrosoftWebDriver.exe"
        self.__guid = str(uuid.uuid4())
        if os.path.exists(os.getcwd()):
            for root, dirs, files in os.walk(os.getcwd()):
                for fn in files:
                    if fn.__contains__(self.task_info['key_word'] + ".log"):
                        self.__guid = str(fn[0:fn.index(str(self.task_info['key_word'] + '.'))])
                        break
        self.Http = HttpRequestBase.HttpRequestBase(timeout=200, retries=20)
        self.rdb = redis.Redis(host="localhost", port=6379, db=0)
        self.cond = threading.Condition()
        self.face_detector = FaceRecognition(self.task_info["module_save_path"])
        self.tmp_save_path = os.getcwd() + "\\tmp_image_save"
        self.image_save_path = self.task_info["image_save_path"] if self.task_info.keys().__contains__(
            "image_save_path") else os.getcwd() + "\\image_save_path"
        if self.image_save_path is None:
            self.image_save_path = os.getcwd() + "\\image_save_path"
        if not os.path.exists(self.tmp_save_path):
            os.makedirs(self.tmp_save_path)
        if not os.path.exists(self.image_save_path):
            os.makedirs(self.image_save_path)

    def claw_by_face_recognition(self):
        # init image queue
        index = self.__get_current_index()
        print("Get images url from network or redis caches")
        if index < 1:
            if os.path.isfile(self.__guid + self.task_info['key_word'] + ".log"):
                os.remove(self.__guid + self.task_info['key_word'] + ".log")
            self.__search_image_url_by_browser()
        else:
            index = index + 1
            print("断点续传...")
            self.image_url_queue = eval(set(self.rdb.smembers("image_list_" + self.__guid)).pop())[index:]
        self.backup_queue = eval(set(self.rdb.smembers("image_list_" + self.__guid)).pop())
        self.image_url_queue.reverse()
        # train knn with samples
        if self.task_info["module_save_path"] is None:
            print("Start training knn by Samples")
            if dict(self.task_info).keys().__contains__("sample_dir"):
                self.face_detector.knn_train(self.task_info["sample_dir"], n_neighbors=2)
            else:
                return
            print("Knn is ready to use")
        else:
            print("Using exits knn module")
        # start claw threads
        print("Starting image claw threads")
        for each in range(4):
            job_guid = str(uuid.uuid4())
            self.__threads[job_guid] = self.ClawThread(self.cond, job_guid, self)
            self.__threads[job_guid].start()
        while True:
            status = []
            for thread in self.__threads.values():
                status.append(thread.is_alive())
            if status.count(False) == 4:
                break
            else:
                del status
            time.sleep(10)
        downloaded = len(os.listdir(self.image_save_path))
        miss = self.nums - downloaded
        print("\nDownloaded image count %d ,missed count %d" % (downloaded, miss))

    def __search_image_url_by_browser(self):
        driver = webdriver.WebDriver(executable_path=self.edge_driver,  port=17556)
        driver.maximize_window()
        driver.get(self.server.format(self.task_info["key_word"]))
        height = 0
        time.sleep(3)
        while True:
            driver.execute_script("window.scrollBy(0,1000);")
            time.sleep(1)
            tmp = driver.execute_script("var q=document.body.scrollHeight;return(q)")
            if tmp > height:
                height = tmp
                continue
            else:
                time.sleep(3)
                driver.execute_script("window.scrollBy(0,1000);")
                time.sleep(1)
                tmp = driver.execute_script("var s=document.body.scrollHeight;return(s);")
                if tmp > height:
                    height = tmp
                    continue
                else:
                    break
        image_parse = BeautifulSoup(driver.page_source, "html.parser")
        image_pages = image_parse.find_all("div", class_="imgpage")
        driver.close()
        for page in image_pages:
            page_parse = BeautifulSoup(str(page), "html.parser")
            lis = page_parse.find_all("li", class_="imgitem")
            for li in lis:
                self.image_url_queue.append(str(li.get("data-objurl")))
        del driver, image_parse, image_pages
        self.nums = len(self.image_url_queue)
        self.rdb.sadd("image_list_" + self.__guid, self.image_url_queue)

    def search_image_url_by_request(self):
        res = self.Http.requests_request('GET', self.request_image.format("'" + self.task_info['key_word'] + "'", 0))

        page_dic = self.json_parser(res)
        count = page_dic['listNum']
        del res, page_dic
        for i in range(0, count, 30):
            res = self.Http.requests_request('GET',
                                             self.request_image.format("'" + self.task_info['key_word'] + "'", i))
            images = self.json_parser(res).get('data')
            for each in images:
                if dict(each).__contains__("middleURL"):
                    self.image_url_queue.append(each["middleURL"])
        self.nums = len(self.image_url_queue)
        self.rdb.sadd("image_list_" + self.__guid, self.image_url_queue)

    @staticmethod
    def json_parser(json_str):
        try:
            replacedJson = re.sub(r'(?<!\\)\\(?!["\\/bfnrt]|u[0-9a-fA-F]{4})', r"", json_str)
            json_dic = json.loads(replacedJson)
            return json_dic
        except json.decoder.JSONDecodeError:
            return {}

    def __get_current_index(self):
        last_line = ""
        if os.path.isfile(self.__guid + self.task_info['key_word'] + ".log"):
            with open(self.__guid + self.task_info['key_word'] + ".log", 'rb') as f:
                first_line = f.readline()  # 读第一行
                if first_line == bytes("", encoding="utf-8") or len(first_line) < 60:
                    return 0
                off = -100  # 设置偏移量
                while True:
                    f.seek(off, 2)  # seek(off, 2)表示文件指针：从文件末尾(2)开始向前50个字符(-50)
                    lines = f.readlines()  # 读取文件指针范围内所有行
                    if len(lines) >= 2:  # 判断是否最后至少有两行，这样保证了最后一行是完整的
                        last_line = lines[-1]  # 取倒数第二行
                    else:
                        last_line = lines[0]
                        break
                    off *= 2
                f.flush()
                f.close()
            start = last_line.index(bytes('[', "utf-8"))
            end = last_line.index(bytes(']', "utf-8"))
            index = last_line[start + 1:end]
            return int(index)
        else:
            return 0

    def log_writer(self, status):
        with open(self.__guid + self.task_info['key_word'] + ".log", 'a+', encoding='utf-8') as f:
            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            f.writelines(current_time + "--"
                         + "NowDownloading:["
                         + status.get("index")
                         + "]--url:" + status.get("url"))
            f.write('\n')
            f.flush()
            f.close()
        del f

    def claw_recognition(self, url):
        tmp_file_name = self.tmp_save_path + "\\" + str(uuid.uuid4()) + ".jpg"
        try:
            res = self.Http.url_download(tmp_file_name, url, show_progress=False)
            if res:
                result = self.face_detector.image_recognition(tmp_file_name)
                if len(result) > 0:
                    if result[0]["name"] == self.task_info["key_word"]:
                        shutil.move(tmp_file_name, self.image_save_path)
        except FileNotFoundError:
            pass

    class ClawThread(threading.Thread):
        def __init__(self, cond, name, out):
            self.out = out
            super(self.__class__, self).__init__()
            self.cond = cond
            self.name = name

        def run(self):
            while len(self.out.image_url_queue) > 0:
                self.cond.acquire()
                url = ""
                current_index = 0
                if len(self.out.image_url_queue) > 0:
                    url = self.out.image_url_queue.pop()
                    current_index = self.out.backup_queue.index(url)

                    self.out.log_writer({"index": str(current_index), "url": url})
                self.cond.release()
                self.out.claw_recognition(url)
                res = {
                    "percent": float((current_index + 1) * 100 / self.out.nums),
                    "num": int(current_index)
                }
                sys.stdout.write(
                    "\r" + "已下载:%.3f%%，正在下载：第%dp，耗时：%s秒" % (
                        res["percent"], res["num"], round(time.process_time(), 2)))


task = {
    "key_word": "Kayden Kross",
    "sample_dir": "f:\\train",
    "module_save_path": None,
    "image_save_path": "F:\\down"
}
a = ImageClaw(task_info=task)
a.claw_by_face_recognition()
