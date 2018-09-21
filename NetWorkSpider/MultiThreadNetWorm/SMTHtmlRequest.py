# -*- coding:UTF-8 -*-
import datetime
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import redis
from bs4 import BeautifulSoup

from BasicLibrarys.Common import HttpRequestBase, OracleDBOP


class DownloadQueue:
    def __init__(self, task_info):
        self.task_info = task_info
        self.backup_queue = []
        self.backup_url = []
        self.queue_names = []  # 存放章节名
        self.queue_urls = []  # 存放章节链接
        self.nums = 0
        self.__threads = {}
        self.__guid = str(uuid.uuid4())
        if os.path.exists(os.getcwd()):
            for root, dirs, files in os.walk(os.getcwd()):
                for fn in files:
                    if fn.__contains__(self.task_info['bookname'] + ".log"):
                        self.__guid = str(fn[0:fn.index(str(self.task_info['bookname'] + '.'))])
                        break
        self.db = OracleDBOP.OracleOP("localhost", 1521, "OCDB", "NWSYS", "NW123456")
        self.Http = HttpRequestBase.HttpRequestBase(timeout=20, retries=20, redirect=True)
        self.rdb = redis.Redis(host="localhost", port=6379, db=0)
        self.cond = threading.Condition()

    def download(self):
        html = self.Http.requests_request("GET", self.task_info['target'])
        self.chapter_list_url_get(html)
        self.backup_queue = self.queue_names.copy()
        self.backup_url = self.queue_urls.copy()
        self.nums = len(self.queue_urls)
        index = self.get_current_index()
        if index < 1 and not os.path.isfile(self.task_info['path']):
            if os.path.isfile(self.__guid + self.task_info['bookname'] + ".log"):
                os.remove(self.__guid + self.task_info['bookname'] + ".log")
            self.book_title_writer(self.task_info['path'], self.task_info['bookname'])
            index = 0
        else:
            index = index + 11
            print("断点续传...")
        self.queue_urls = self.queue_urls[index:]
        self.queue_names = self.queue_names[index:]
        self.queue_names.reverse()
        self.queue_urls.reverse()
        if len(self.queue_names) < 1:
            return
        # self.thread_pool_process()
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
        self.process_save()

    def process_save(self):
        print("\nchecking miss chapters...")
        self.download_miss()
        print("writing file...")
        count = 0
        for each in self.backup_queue:
            text = self.rdb.get(name=each)
            if text is not None and text != "":
                count = count + len(text)
                self.book_content_writer(self.task_info['path'], str(text, encoding="utf-8"))
                sys.stdout.write("\r" + "写入量：%.3fKB" % float(count / 1024.0))
            else:
                print("miss:" + each)
        print("Done!")
        if input("\n是否保存至数据库？Y/N：").upper() == "Y":
            if self.DBQuery(self.task_info['bookname']):
                self.DBUpdate()
            else:
                self.DBInsert()
        if os.path.isfile(self.__guid + self.task_info['bookname'] + ".log") and input("\n是否删除日志？Y/N：").upper() == "Y":
            os.remove(self.__guid + self.task_info['bookname'] + ".log")
            self.rdb.flushall()

    def chapter_list_url_get(self, html):
        if html is None:
            return
        bes_dl = BeautifulSoup(html, "html.parser")
        texts = []
        if self.task_info['list_identify'] == 'class':
            texts = bes_dl.find_all(self.task_info['list_type'], class_=self.task_info['list_identify_text'])
        elif self.task_info['list_identify'] == 'id':
            texts = bes_dl.find_all(self.task_info['list_type'], id=self.task_info['list_identify_text'])
        if len(texts) < 1:
            return
        bes = BeautifulSoup(str(texts[0]), "html.parser")
        list_dd = bes.find_all(self.task_info['list_element'])
        for each in list_dd:
            bes_list = BeautifulSoup(str(each), "html.parser")
            list_a = bes_list.find_all("a")
            if len(list_a) > 0:
                chapter_url = list_a[0].get("href")
                if list_a[0].get("title") is not None:
                    chapter_name = list_a[0].get("title")
                else:
                    chapter_name = list_a[0].contents[0]
                if (chapter_url is not None) and (chapter_name is not None):
                    self.queue_names.append(chapter_name)
                    self.queue_urls.append(chapter_url)
                else:
                    continue
            else:
                continue

    def get_chapter(self, url, name):
        book = ""
        content = self.Http.requests_request("GET", url)
        if content is None or content == "":
            return book
        bes = BeautifulSoup(content, "html.parser")
        texts = []
        if self.task_info['content_identify'] == 'class':
            texts = bes.find_all(self.task_info['content_type'], class_=self.task_info['content_identify_text'])
        elif self.task_info['content_identify'] == 'id':
            texts = bes.find_all(self.task_info['content_type'], id=self.task_info['content_identify_text'])
        if len(texts) > 0:
            book = self.text_modify(texts)
            self.rdb.set(name, name + "\n" + book)
            if book is None:
                print("false")
        else:
            print("false")
        del bes, content, texts, book

    @staticmethod
    def text_modify(texts):
        text = ""
        for each in texts[0].contents:
            if str(each).__contains__("br"):
                continue
            elif str(each).__contains__("h5"):
                continue
            elif str(each).__contains__("h4"):
                continue
            elif str(each).__contains__("h3"):
                continue
            elif str(each).__contains__("ul"):
                continue
            elif str(each).__contains__("span"):
                continue
            elif str(each).__contains__("<b>"):
                continue
            elif str(each).__contains__("li"):
                continue
            else:
                text = text + str(each).replace('\xa0', '  ').replace(
                    "（乡/\村/\小/\说/\网 wｗw.xiａngcｕnxiaｏsｈuo.cｏm）", "")
            del each
        del texts
        return text

    @staticmethod
    def book_title_writer(path, bookname):
        if os.path.isfile(path):
            os.remove(path)
        with open(path, 'a', encoding='utf-8') as f:
            f.writelines(bookname)
            f.write('\n\n')
            f.flush()
            f.close()

    @staticmethod
    def book_content_writer(path, text):
        with open(path, 'a', encoding='utf-8') as f:
            f.writelines(text)
            f.write('\n\n')
            f.flush()
            f.close()

    def log_writer(self, status):
        with open(self.__guid + self.task_info['bookname'] + ".log", 'a+', encoding='utf-8') as f:
            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            f.writelines(current_time + "--"
                         + "NowDownloading:["
                         + status.get("index")
                         + "]--url:" + status.get("url"))
            f.write('\n')
            f.flush()
            f.close()
        del f

    def get_current_index(self):
        last_line = ""
        if os.path.isfile(self.__guid + self.task_info['bookname'] + ".log"):
            with open(self.__guid + self.task_info['bookname'] + ".log", 'rb') as f:
                first_line = f.readline()  # 读第一行
                if first_line == bytes("", encoding="utf-8") or len(first_line) < 60:
                    return 0
                off = -50  # 设置偏移量
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

    def download_miss(self):
        miss = {}
        for each in self.backup_queue:
            text = self.rdb.get(name=each)
            if text is None or text == each + "\n":
                miss[each] = self.backup_url[self.backup_queue.index(each)]
        for mis in miss.keys():
            self.get_chapter(self.task_info['server'] + miss[mis], mis)
            print("download miss chapter：" + mis)

    def DBQuery(self, bookname):
        r = self.db.query("NW_BOOKSTORE", ["BOOKNAME"], {"BOOKNAME": bookname}, [])
        for each in r:
            if len(each) == 1:
                return True
            else:
                return False

    def DBExport(self, bookname, path):
        r = self.db.query("NW_BOOKSTORE", ["BOOKNAME", "BOOKBLOB"], {"BOOKNAME": bookname}, [])
        for each in r:
            b = each[1].read()
            with open(path, "wb") as f:
                f.write(b)
                f.flush()
                f.close()
        print("导出完成")
        return True

    def DBInsert(self):
        dic = {"ID": str(self.__guid), "BOOKNAME": self.task_info['bookname'], "DOWNLOADURL": self.task_info['target'],
               "CHPCOUNT": len(self.backup_queue), "CREATETIME": datetime.datetime.now()}
        with open(self.task_info['path'], 'rb') as f:
            content = f.read()
            dic["BOOKBLOB"] = content
            dic["BOOKSIZE"] = len(content)
            f.close()
        self.db.insert("NW_BOOKSTORE", dic)
        self.db.close()

    def DBUpdate(self):
        dic = {"ID": str(self.__guid), "BOOKNAME": self.task_info['bookname'], "DOWNLOADURL": self.task_info['target'],
               "CHPCOUNT": len(self.backup_queue), "CREATETIME": ":CREATETIME"}
        with open(self.task_info['path'], 'rb') as f:
            content = f.read()
            dic["BOOKBLOB"] = ":BOOKBLOB"
            dic["BOOKSIZE"] = len(content)
            f.close()
        self.db.update("NW_BOOKSTORE", dic, {"BOOKNAME": self.task_info['bookname']},
                       content={"BOOKBLOB": content, "CREATETIME": datetime.datetime.now()})
        self.db.close()

    def thread_pool_process(self):
        items = []
        for i in range(len(self.queue_urls)):
            items.append((self, self.queue_urls[i], self.queue_names[i]))
        pool = ThreadPoolExecutor(max_workers=5)
        result = pool.map(self.pool_per_process, items.__iter__())
        return [res for res in result]

    @staticmethod
    def pool_per_process(item):
        item[0].get_chapter(item[0].task_info['server'] + item[1], item[2])
        current_index = item[0].queue_names.index(item[2])
        res = {
            "percent": float((current_index + 1) * 100 / item[0].nums),
            "num": int(current_index)
        }
        sys.stdout.write(
            "\r" + "已下载:%.3f%%，正在下载：第%d章，耗时：%s秒" % (
                res["percent"], res["num"], round(time.process_time(), 2)))
        item[0].log_writer({"index": str(current_index), "url": item[1]})

    class ClawThread(threading.Thread):
        def __init__(self, cond, name, out):
            self.out = out
            super(self.__class__, self).__init__()
            self.cond = cond
            self.name = name

        def run(self):
            while len(self.out.queue_urls) > 1:
                self.cond.acquire()
                url = ""
                name = ""
                current_index = 0
                res = {}
                if len(self.out.queue_urls) > 1:
                    url = self.out.queue_urls.pop()
                    name = self.out.queue_names.pop()
                    current_index = self.out.backup_queue.index(name)
                    res = {
                        "percent": float((current_index + 1) * 100 / self.out.nums),
                        "num": int(current_index)
                    }
                    sys.stdout.write(
                        "\r" + "已下载:%.3f%%，正在下载：第%d章，耗时：%s秒" % (
                            res["percent"], res["num"], round(time.process_time(), 2)))
                    self.out.log_writer({"index": str(current_index), "url": url})
                self.cond.release()
                self.out.get_chapter(self.out.task_info['server'] + url, name)
                del url, name, current_index, res
