# -*- coding:UTF-8 -*-
import threading
import uuid

from NetWorkSpider.MultiThreadNetWorm.SSTHtmlRequest import BookDownload

"""
Introduction :This class is design for claw books from any online book sites.
              To using this Class, you should have some Python3.6.3 knowledge
User's Guide :When using this Class to claw html content,please check the target
              html structure first.And after this,you should make difference in Class 
              SingleThreadHtmlRequest.py, then modify the method ChapterListURLGet 
              and getChapter to correctly claw the content according to the target
              html structure.At the end,building the requests parameters and saving
              paths of the MultiThreadDownload class.
              You can also modify the ThreadJob method to Insert or Export books 
              from an Oracle Database. But,at the fist time, you should configure 
              a Database Source of the Program, and change the self parameters in 
              Class OracleDBOP.py
Author :Chen si
Create :2017/11/21
LastModify :2017/11/22
"""


class MultiThreadDownload:
    def __init__(self, tasks):
        self.tasks = tasks
        self.guids = {}
        self.downloads = {}
        self.locks = {}
        self.threads = {}
        if len(tasks) > 0:
            for task in tasks:
                task["guid"] = str(uuid.uuid4())
                itemdownload = BookDownload(task["server"], task["target"], task["save_path"], task["name"])
                self.downloads[task["guid"]] = itemdownload
                self.threads[task["guid"]] = None
                self.guids[task["guid"]] = itemdownload.getGUID()
                self.locks[task["guid"]] = threading.Condition()
        else:
            pass

    def ThreadJob(self, guid):
        itemDownload = self.downloads[guid]
        itemDownload.download()
        # itemDownload.DBInsert()
        # itemDownload.DBExport()

    class JobThread(threading.Thread):
        out = None

        def __init__(self, cond, name, jobname, out):
            self.out = out
            super(out.JobThread, self).__init__()
            self.cond = cond
            self.name = name
            self.jobname = jobname

        def run(self):
            self.cond.acquire()
            self.out.ThreadJob(self.jobname)
            self.cond.release()

    def ThreadBuilding(self):
        for task in self.tasks:
            self.threads[task["guid"]] = self.JobThread(self.locks[task["guid"]], self.guids[task["guid"]],
                                                        task["guid"], self)

    def ThreadStarting(self):
        for thread in list(self.threads.values()):
            thread.start()

    def ThreadAwaking(self, name):
        if name == "all":
            for lock in list(self.locks.values()):
                lock.notifyAll()
        else:
            for task in self.tasks:
                if name == task["name"]:
                    self.locks[task["guid"]].notify()
                else:
                    pass

    def ThreadWaiting(self, name, timeout=None):
        if name == "all":
            for lock in list(self.locks.values()):
                if timeout is not None:
                    lock.wait(timeout)
                else:
                    lock.wait()
        else:
            for task in self.tasks:
                if name == task["name"]:
                    if timeout is not None:
                        self.locks[task["guid"]].wait(timeout)
                    else:
                        self.locks[task["guid"]].wait()
                else:
                    pass

    def ThreadStop(self, name):
        if name == "all":
            for thread in list(self.threads.values()):
                thread.abort()
        else:
            for task in self.tasks:
                if name == task["name"]:
                    self.threads[task["guid"]].abort()
                else:
                    pass
