# -*- coding:UTF-8 -*-
import datetime
import os
import sys
import time
import uuid
from bs4 import BeautifulSoup
from BasicLibrarys.Common import HttpRequestBase, OracleDBOP


class BookDownload:
    def __init__(self, server, target, path, bookname):
        self.server = server
        self.target = target
        self.__names = []  # 存放章节名
        self.__urls = []  # 存放章节链接
        self.__nums = 0
        self.path = path
        self.bookname = bookname
        self.__guid = str(uuid.uuid4())
        if os.path.exists(os.getcwd()):
            for root, dirs, files in os.walk(os.getcwd()):
                for fn in files:
                    if fn.__contains__(bookname + ".log"):
                        self.__guid = str(fn[0:fn.index(str(self.bookname + '.'))])
                        break
        self.db = OracleDBOP.OracleOP("localhost", 1521, "OCDB", "NWSYS", "NW123456")
        self.Http = HttpRequestBase.HttpRequestBase(timeout=200, retries=20)

    def getGUID(self):
        return self.__guid

    def download(self):
        html = self.Http.requests_request("GET", self.target)
        self.ChapterListURLGet(html)
        self.__nums = len(self.__names)
        index = self.GetCurrentIndex()
        if index < 1 or not os.path.isfile(self.path):
            if os.path.isfile(self.__guid + self.bookname + ".log"):
                os.remove(self.__guid + self.bookname + ".log")
                index = 0
            self.BookTitleWriter(self.path, self.bookname)
        else:
            index = index + 1
            print("断点续传...")
        for each in range(index, self.__nums):
            url = self.__urls[each]
            name = self.__names[each]
            text = self.getChapter(self.server + url, name)
            self.BookContentWriter(self.path, text)
            self.LogWriter({"index": str(each), "url": url})
            res = {
                "percent": float((each + 1) * 100 / self.__nums),
                "num": int(each + 1)
            }
            sys.stdout.write('\r' + "已下载:%.3f%%，正在下载：第%d章，%s" % (res["percent"], res["num"], name))
        if os.path.isfile(self.__guid + self.bookname + ".log") and input("\n是否删除日志？Y/N：").upper() == "Y":
            os.remove(self.__guid + self.bookname + ".log")

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
        dic = {"ID": str(self.__guid), "BOOKNAME": self.bookname, "DOWNLOADURL": self.target,
               "CHPCOUNT": len(self.__names), "CREATETIME": datetime.datetime.now()}
        with open(self.path, 'rb') as f:
            content = f.read()
            dic["BOOKBLOB"] = content
            dic["BOOKSIZE"] = len(content)
            f.close()
        self.db.insert("NW_BOOKSTORE", dic)
        self.db.close()

    def DBUpdate(self):
        dic = {"ID": str(self.__guid), "BOOKNAME": self.bookname, "DOWNLOADURL": self.target,
               "CHPCOUNT": len(self.__names), "CREATETIME": datetime.datetime.now()}
        with open(self.path, 'rb') as f:
            content = f.read()
            dic["BOOKBLOB"] = ":BOOKBLOB"
            dic["BOOKSIZE"] = len(content)
            f.close()
        self.db.update("NW_BOOKSTORE", dic, {"BOOKNAME": self.bookname}, content=content)
        self.db.close()

    def ChapterListURLGet(self, html):
        if html is None:
            return
        besdl = BeautifulSoup(html, "html.parser")
        texts = besdl.find_all("dl")
        if len(texts) < 1:
            return
        bes = BeautifulSoup(str(texts[0]), "html.parser")
        listdd = bes.find_all("dd")
        for each in listdd:
            beslist = BeautifulSoup(str(each), "html.parser")
            lista = beslist.find_all("a")
            if len(lista) > 0:
                chapterURL = lista[0].get("href")
                chapterName = lista[0].contents[0]
                if (chapterURL is not None) and (chapterName is not None):
                    self.__names.append(chapterName)
                    self.__urls.append(chapterURL)
                else:
                    print("none")
                    continue
            else:
                continue

    def getChapter(self, url, name):
        book = ""
        content = self.Http.requests_request("GET", url)
        if content is None:
            return ""
        bes = BeautifulSoup(content, "html.parser")
        texts = bes.find_all("div", class_="showtxt")
        if len(texts) > 0:
            book = self.text_modify(texts)
        return name + "\n" + book

    def text_modify(self, texts):
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
        return text

    def BookTitleWriter(self, path, bookname):
        write_flag = True
        if os.path.isfile(path):
            os.remove(path)
        with open(path, 'a', encoding='utf-8') as f:
            f.writelines(bookname)
            f.write('\n\n')
            f.flush()
            f.close()

    def BookContentWriter(self, path, text):
        write_flag = True
        with open(path, 'a', encoding='utf-8') as f:
            f.writelines(text)
            f.write('\n\n')
            f.flush()
            f.close()

    def LogWriter(self, status):
        with open(self.__guid + self.bookname + ".log", 'a', encoding='utf-8') as f:
            currenttime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            f.writelines(currenttime + "--"
                         + "NowDownloading:["
                         + status.get("index")
                         + "]--url:" + status.get("url"))
            f.write('\n')
            f.flush()
            f.close()

    def GetCurrentIndex(self):
        last_line = ""
        if os.path.isfile(self.__guid + self.bookname + ".log"):
            with open(self.__guid + self.bookname + ".log", 'rb') as f:
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
