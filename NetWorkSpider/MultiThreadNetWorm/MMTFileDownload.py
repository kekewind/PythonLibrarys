import os
import multiprocessing
from bs4 import BeautifulSoup
from BasicLibrarys.Common import HttpRequestBase


class BigFileDownload:
    def __init__(self, target, directory):
        self.target = target
        self.__names = []
        self.__urls = []
        self.__nums = 0
        self.path = directory
        self.processes = []
        self.Http = HttpRequestBase.HttpRequestBase(timeout=200, retries=20)

    def download(self):
        html = self.Http.requests_request("GET", self.target)
        self.url_quene(html)
        self.__nums = len(self.__urls)
        self.thunder_batch_download()
        # self.multiprocess_download(3)

    def url_quene(self, html):
        if html is None:
            return
        bes_dev = BeautifulSoup(html, "html.parser")
        texts = bes_dev.find_all("div", class_="down_list")
        if len(texts) < 1:
            return
        bes = BeautifulSoup(str(texts[0]), "html.parser")
        list_p = bes.find_all("input", class_="down_url col-md-1")
        if len(list_p) > 0:
            for each in list_p:
                url = str(each.get("value"))
                name = str(each.get("file_name"))
                self.__urls.append(url)
                self.__names.append(name)

    def thunder_batch_download(self):
        for url in self.__urls:
            os.chdir("D:\\Program Files\\Thunder\\Program\\")
            os.system("Thunder.exe -StartType:DesktopIcon \"%s\"" % url)

    def multiprocess_download(self, process_count):
        def process_worker(url, path):
            HttpRequestBase.HttpRequestBase().url_download(path, url)

        for each in range(process_count, self.__nums, process_count):
            for task in range(each):
                if __name__ == '__main__':
                    p = multiprocessing.Process(target=process_worker,
                                                args=(self.__urls[task], self.path + self.__names[task]))
                    self.processes.append(p)
            if __name__ == '__main__':
                self.processes[0].start()
                self.processes[1].start()
                self.processes[2].start()
                while len(self.processes) > 0:
                    for process in self.processes:
                        if not process.is_alive():
                            self.processes.remove(process)
                else:
                    continue


s = BigFileDownload("https://www.lol5s.com/v/10833.html", "")
s.download()
