import asyncio
import datetime
import os
import sys
import time
import uuid

import redis
from bs4 import BeautifulSoup

from BasicLibrarys.Common import HttpRequestBase

task = {
    'server': "https://www.gebiqu.com",
    'target': "https://www.gebiqu.com/biquge_1889/",
    'path': "F:\\迅雷下载\\两世欢.txt",
    'bookname': "两世欢",
    'list_identify': 'id',
    'list_type': 'div',
    'list_identify_text': 'list',
    'list_element': 'dd',
    'content_identify': 'id',
    'content_type': 'div',
    'content_identify_text': 'content'
}
queue_names = []
queue_urls = []
nums = 0
count = [0]
rdb = redis.Redis(host="localhost", port=6379, db=0)
AIOHttp = HttpRequestBase.HttpRequestBase(timeout=200, retries=20, redirect=True)
db = None  # MySQLOP("localhost", 3306, "network_book", "root", "123456")
Http = HttpRequestBase.HttpRequestBase(timeout=2000, retries=20)


def download(bookname, tasks=None):
    global queue_names, queue_urls, task_info, nums, count
    # if False:  # DBQuery(bookname):
    #     # r = db.query("NW_BOOKSTORE", ["TASK"], {"BOOKNAME": bookname}, [])
    #     if r:
    #         task_info = eval(r[0][0])
    #     elif not tasks:
    #         return
    # else:
    task_info = tasks
    html = Http.requests_request("GET", task_info['target'])
    chapter_list_url_get(html)
    nums = len(queue_urls)
    if len(queue_names) < 1:
        return
    book_title_writer(task_info['path'], task_info['bookname'])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    print("\nchecking miss chapters...")
    download_miss()
    print("writing file...")
    counts = 0
    for each in queue_names:
        text = rdb.get(name=each)
        if text is not None and text != "":
            counts = counts + len(text)
            book_content_writer(task_info['path'], str(text, encoding="utf-8"))
            sys.stdout.write("\r" + "写入量：%.3fKB" % float(counts / 1024.0))
        else:
            print("miss:" + each)
    print("Done!")
    if input("\n是否保存至数据库？Y/N：").upper() == "Y":
        if DBQuery(task_info['bookname']):
            DBUpdate()
        else:
            DBInsert()


def chapter_list_url_get(html):
    if html is None:
        return
    bes_dl = BeautifulSoup(html, "html.parser")
    texts = []
    if task_info['list_identify'] == 'class':
        texts = bes_dl.find_all(task_info['list_type'], class_=task_info['list_identify_text'])
    elif task_info['list_identify'] == 'id':
        texts = bes_dl.find_all(task_info['list_type'], id=task_info['list_identify_text'])
    if len(texts) < 1:
        return
    bes = BeautifulSoup(str(texts[0]), "html.parser")
    list_dd = bes.find_all(task_info['list_element'])
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
                queue_names.append(chapter_name)
                queue_urls.append(chapter_url)
            else:
                continue
        else:
            continue


def parse_content(content, name):
    book = ""
    if content is None or content == "":
        print("miss")
        return book
    bes = BeautifulSoup(content, "html.parser")
    texts = []
    if task_info['content_identify'] == 'class':
        texts = bes.find_all(task_info['content_type'], class_=task_info['content_identify_text'])
    elif task_info['content_identify'] == 'id':
        texts = bes.find_all(task_info['content_type'], id=task_info['content_identify_text'])
    if len(texts) > 0:
        book = text_modify(texts)
        rdb.set(name, name + "\n" + book)
        if book is None:
            print("false")
    else:
        print("false")


async def aio_get_chapter(url, name, semaphore):
    async with semaphore:
        content = await AIOHttp.aio_requests("GET", task_info['server'] + url)
        parse_content(content, name)
        count[0] = count[0] + 1
        res = {
            "percent": float((count[0] + 1) * 100 / nums),
            "num": int(count[0])
        }
        sys.stdout.write(
            "\r" + "已下载:%.3f%%，正在下载：第%d章，耗时：%s秒" % (
                res["percent"], res["num"], round(time.process_time(), 2)))


sem = asyncio.Semaphore(500)


async def run():
    args = []
    for i in range(0, len(queue_urls), 1):
        args.append((queue_urls[i], queue_names[i]))
    semaphore = asyncio.Semaphore(10)  # 限制并发量为500
    to_get = [aio_get_chapter(*each, semaphore) for each in args]
    await asyncio.wait(to_get)


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
        elif str(each).__contains__("p"):
            continue
        else:
            text = text + str(each).replace('\xa0', '  ').replace(
                "（乡/\村/\小/\说/\网 wｗw.xiａngcｕnxiaｏsｈuo.cｏm）", "")
        del each
    del texts
    return text


def book_title_writer(path, bookname):
    if os.path.isfile(path):
        os.remove(path)
    with open(path, 'a', encoding='utf-8') as f:
        f.writelines(bookname)
        f.write('\n\n')
        f.flush()
        f.close()


def book_content_writer(path, text):
    with open(path, 'a', encoding='utf-8') as f:
        f.writelines(text)
        f.write('\n\n')
        f.flush()
        f.close()


def download_miss():
    miss = {}
    for each in queue_names:
        text = rdb.get(name=each)
        if text is None or text == each + "\n":
            miss[each] = queue_urls[queue_names.index(each)]
    for mis in miss.keys():
        content = Http.requests_request("GET", task_info['server'] + miss[mis])
        parse_content(content, mis)
        print("download miss chapter：" + mis)


def DBQuery(bookname):
    r = db.query("NW_BOOKSTORE", ["BOOKNAME", "TASK"], {"BOOKNAME": bookname}, [])
    if len(r) == 1:
        return True
    else:
        return False


def DBExport(bookname, path):
    r = db.query("NW_BOOKSTORE", ["BOOKNAME", "BOOKBLOB"], {"BOOKNAME": bookname}, [])
    for each in r:
        b = each[1].read()
        with open(path, "wb") as f:
            f.write(b)
            f.flush()
            f.close()
    print("导出完成")
    return True


def DBInsert():
    dic = {"ID": str(uuid.uuid4()), "BOOKNAME": task_info['bookname'], "DOWNLOADURL": task_info['target'],
           "CHPCOUNT": len(queue_names), "CREATETIME": datetime.datetime.now(), "TASK": str(task_info)}
    with open(task_info['path'], 'rb') as f:
        content = f.read()
        dic["BOOKBLOB"] = content
        dic["BOOKSIZE"] = len(content)
        f.close()
    db.insert("NW_BOOKSTORE", dic, "BOOKBLOB")
    db.close()


def DBUpdate():
    dic = {"ID": str(uuid.uuid4()), "BOOKNAME": task_info['bookname'], "DOWNLOADURL": task_info['target'],
           "CHPCOUNT": len(queue_names), "CREATETIME": datetime.datetime.now(), "TASK": "%s"}
    with open(task_info['path'], 'rb') as f:
        content = f.read()
        dic["BOOKBLOB"] = "%s"
        dic["BOOKSIZE"] = len(content)
        f.close()
    db.update("NW_BOOKSTORE", dic, {"BOOKNAME": task_info['bookname']},
              content={"BOOKBLOB": content, "TASK": str(task_info)},
              blob_key="BOOKBLOB")
    db.close()


if __name__ == "__main__":
    download("两世欢", task)
