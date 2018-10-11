"""
Beauty Girl Claw
|'=============================================================='|
||                                .::::.                        ||
||                              .::::::::.                      ||
||                              :::::::::::                     ||
||                              ':::::::::::..                  ||
||                              .:::::::::::::::'               ||
||                                '::::::::::::::.`             ||
||                                  .::::::::::::::::.'         ||
||                                .::::::::::::..               ||
||                              .::::::::::::::''               ||
||                   .:::.       '::::::::''::::                ||
||                 .::::::::.      ':::::'  '::::               ||
||                .::::':::::::.    :::::    '::::.             ||
||              .:::::' ':::::::::. :::::.     ':::.            ||
||            .:::::'     ':::::::::.::::::.      '::.          ||
||          .::::''         ':::::::::::::::'       '::.        ||
||         .::''              '::::::::::::::'        ::..      ||
||      ..::::                  ':::::::::::'         :'''`     ||
||   ..''''':'                    '::::::.'                     ||
|'=============================================================='|
"""
import re
import sys
import threading
import time
from logging import Logger, INFO, Formatter
from logging.handlers import RotatingFileHandler

import requests
import threadpool
from bs4 import BeautifulSoup
from threadpool import ThreadPool

from BasicLibrarys.Common import HttpRequestBase, MongoDBOP
from BasicLibrarys.Common.RegularExFix import file_path_fix, url_fix, download_url_check


class TorrentsClaw:
    def __init__(self, task_info):
        self.__threads = {}
        self.task_info = task_info
        self.urls_queue = {}
        self.mdb = MongoDBOP.MongoDBOP('127.0.0.1', 27017, 'torrents')
        self.Http = HttpRequestBase.HttpRequestBase(timeout=200, retries=20)
        self.progress = 0
        self.total_count = 0
        self.damage = [0]
        self.t_pool = ThreadPool(16)
        self.logger = Logger(__name__)
        self.logger.setLevel(INFO)
        f_handler = RotatingFileHandler(filename="t_claw.log", maxBytes=2048 * 1024, backupCount=3,
                                        encoding='utf-8')
        f_handler.setFormatter(Formatter("%(asctime)s  %(filename)s : %(levelname)s  %(message)s"))
        self.logger.addHandler(f_handler)
        self.tmp_queue = []
        self.cond = threading.Condition()

    # 论坛页面数量获取
    def get_pages(self):
        html = self.Http.requests_request('GET', self.task_info['target'].format(1))
        if html == '':
            return 0
        bes_page = BeautifulSoup(html, "html.parser")
        page_txt = bes_page.find_all('div', class_='pages')
        if len(page_txt) == 0:
            return 0
        page_parser = re.compile(r"/\d* total \)")
        page = int(page_parser.findall(str(page_txt[0]))[0].replace(' total )', '').replace('/', ''))
        del html, bes_page, page_txt, page_parser
        return page

    # 抓取论坛帖子地址
    def page_claw(self):
        res = self.mdb.find_by_kvpair('logs', {})[0]
        current_page = int(res['current_page'])
        current_index = int(res['current_index'])
        total = int(res['total'])
        is_done = res['isDone']
        cur = -1
        if current_index == 0 and is_done:
            current_page = current_page + 1
        if current_index != 0:
            current_index = current_index + 1
        for index in range(current_page, self.get_pages(), 1):
            self.mdb.update('logs', {}, '$set', {'current_page': index, 'isDone': False})
            page_html = self.Http.requests_request('GET', self.task_info['target'].format(index + 1), True)
            if page_html == '':
                # page_html = self.Http.requests_request('GET', self.task_info['target'].format(index + 1), True)
                continue
            bes_page = BeautifulSoup(page_html, "html.parser")
            page_txt = bes_page.find_all('div', class_='pages')
            cur_page = BeautifulSoup(str(page_txt), "html.parser")
            cur_tmp = int(str(cur_page.find('b').contents[0]).replace(' ', ''))
            if cur == cur_tmp:
                break
            else:
                cur = cur_tmp
            if self.task_info['list_identify'] == 'class':
                article_list = bes_page.find_all(self.task_info['list_type'],
                                                 class_=self.task_info['list_identify_text'])
            else:
                article_list = bes_page.find_all(self.task_info['list_type'], id=self.task_info['list_identify_text'])
            if len(article_list) == 0:
                continue
            old_count = total
            for each in range(current_index, len(article_list), 1):
                if article_list[each].text.__contains__('= = = = ='):
                    continue
                bes_items = BeautifulSoup(str(article_list[each]), "html.parser")
                items = bes_items.find('h3')
                if items is None:
                    continue
                bes_target = BeautifulSoup(str(items), "html.parser")
                target = bes_target.find('a')
                if target is None:
                    continue
                if target.get('href') is not None and target.text != '':
                    info = {
                        'url': target.get('href'),
                        'name': target.text,
                        'timestamp': time.time(),
                        'identify': '', 'preview': '', 'torrent': '', 'img_url': ''
                    }
                    self.mdb.insert('resources', info)
                del bes_items, items, bes_target, target
                total = total + 1
                self.mdb.update('logs', {}, '$set', {'current_index': each, 'total': total})
            del page_html, bes_page, article_list
            self.mdb.update('logs', {}, '$set', {'current_page': index, 'current_index': 0, 'isDone': True})
            print(f'Page {index + 1} extract complete, new urls {total-old_count}, total {total}')
        res = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'ASC']])
        for each in res:
            if not self.urls_queue.__contains__(each['url']):
                self.urls_queue[each['url']] = str(each['_id'])
            else:
                self.mdb.delete('resources', {'_id': each['_id']})
        self.urls_queue = {}
        res = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'ASC']])
        for each in res:
            if not self.urls_queue.__contains__(each['name']):
                self.urls_queue[each['name']] = str(each['_id'])
            else:
                self.mdb.delete('resources', {'_id': each['_id']})
        self.mdb.update('logs', {}, '$set', {'total': len(self.urls_queue.keys())})
        print('Page claw done')

    # 帖子网盘id抓取线程配置
    def identify_claw(self):
        res = self.mdb.find_by_kvpair('logs', {})[0]
        p_cur_index = int(res['p_cur_index'])
        self.progress = p_cur_index
        self.total_count = self.mdb.find_count('resources', {})
        queue = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'ASC']], limit=100,
                                        skip=p_cur_index)
        if len(queue) == 0:
            return
        total = len(queue) + p_cur_index
        workers = threadpool.makeRequests(self.get_unique, queue)
        [self.t_pool.putRequest(req, block=False) for req in workers]
        self.t_pool.wait()
        del queue, workers, res
        self.mdb.update('logs', {}, '$set', {'p_cur_index': total})
        self.identify_claw()

    # 帖子网盘id及缩略图抓取worker
    def get_unique(self, job):
        try:
            html = self.Http.requests_request('GET', self.task_info['server'] + job['url'])
        except:
            self.logger.warning('timeout')
            self.damage[0] = self.damage[0] + 1
            return None
        bes_content = BeautifulSoup(html, "html.parser")
        uid_str = 'Missed'
        if self.task_info['content_identify'] == 'class':
            content = bes_content.find(self.task_info['content_type'],
                                       class_=self.task_info['content_identify_text'])
        else:
            content = bes_content.find(self.task_info['content_type'], id=self.task_info['content_identify_text'])
        if content is not None:
            bes_info = BeautifulSoup(str(content), 'html.parser')
            img = bes_info.find_all('img')
            uid_parser = re.compile(r"link.php\?ref=\w*")
            uid_pat = uid_parser.findall(str(content))
            if len(img) > 0 and len(uid_pat) > 0:
                uid_str = uid_pat[0].replace('link.php?ref=', '')
                self.mdb.update('resources', {'url': job['url']}, '$set', {'identify': uid_str})
            else:
                self.damage[0] = self.damage[0] + 1
                self.logger.warning('identify_miss')
                self.mdb.delete('resources', {'url': job['url']})
                self.mdb.update('logs', {}, '$inc', {'total': -1})
            del bes_info, img, uid_parser, uid_pat
        else:
            self.damage[0] = self.damage[0] + 1
            self.logger.warning('content_miss')
        self.progress = self.progress + 1
        percent = self.progress / self.total_count * 100
        sys.stdout.write(
            "\r" + f"已下载:{self.progress}/{str(round(percent,4))}%, Missed:{self.damage[0]}, UniqueId:{uid_str}")
        del content, bes_content, html

    # 帖子缩略图抓取线程配置
    def previews_claw(self):
        res = self.mdb.find_by_kvpair('logs', {})[0]
        p_cur_index = int(res['p_cur_index'])
        self.progress = p_cur_index
        self.total_count = self.mdb.find_count('resources', {})
        queue = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'ASC']], limit=100,
                                        skip=p_cur_index)
        if len(queue) == 0:
            return
        total = len(queue) + p_cur_index
        workers = threadpool.makeRequests(self.get_image_url, queue)
        [self.t_pool.putRequest(req, block=False) for req in workers]
        self.t_pool.wait()
        del queue, workers, res
        self.mdb.update('logs', {}, '$set', {'p_cur_index': total})
        self.previews_claw()

    # 帖子缩略图抓取线程worker
    def get_image_url(self, job):
        try:
            html = self.Http.requests_request('GET', self.task_info['server'] + job['url'])
        except:
            self.logger.warning('timeout')
            self.damage[0] = self.damage[0] + 1
            return None
        bes_content = BeautifulSoup(html, "html.parser")
        uid_str = 'Missed'
        if self.task_info['content_identify'] == 'class':
            content = bes_content.find(self.task_info['content_type'],
                                       class_=self.task_info['content_identify_text'])
        else:
            content = bes_content.find(self.task_info['content_type'], id=self.task_info['content_identify_text'])
        if content is not None:
            bes_info = BeautifulSoup(str(content), 'html.parser')
            img = bes_info.find_all('img')
            if len(img) > 0 and job['identify'] != '':
                uid_str = job['identify']
                self.save_image(img, uid_str, job)
            del bes_info, img
        else:
            self.logger.warning('content miss')
            self.damage[0] = self.damage[0] + 1
            return None
        self.progress = self.progress + 1
        percent = self.progress / self.total_count * 100
        sys.stdout.write(
            "\r" + f"已下载:{self.progress}/{str(round(percent,4))}%, Missed:{self.damage[0]}, UniqueId:{uid_str}")
        del content, bes_content, html

    # 抓取缩略图并存储
    def save_image(self, img, uid_str, job):
        url_parser = re.compile(r"[a-zA-z]+://[^\s]*")
        img_url = []
        for tmp in img:
            if len(url_parser.findall(tmp.get('src'))) > 0:
                if download_url_check(str(tmp.get('src'))):
                    img_url.append(url_fix(str(tmp.get('src'))))
        if len(img_url) == 0:
            self.damage[0] = self.damage[0] + 1
            self.logger.warning('preview_bad')
            return None
        r, cursor = self.ref(img_url, 0)
        self.mdb.update('resources', {'identify': uid_str}, '$set', {'img_url': img_url[cursor]})
        ext = str(img_url[cursor])[str(img_url[cursor]).rindex('.'):]
        if r is None:
            self.damage[0] = self.damage[0] + 1
            self.logger.warning('preview_damage')
        elif r.status_code == 404:
            self.damage[0] = self.damage[0] + 1
            self.logger.warning(img_url[cursor] + f'--[{uid_str}]' + '--preview_miss')
        elif self.task_info['save_type'] == 'db' and len(r.content) > 1024:
            check_exits = self.mdb.find_by_kvpair('resources', {'identify': uid_str})
            if len(check_exits) > 0:
                if check_exits[0]['preview'] != "":
                    self.mdb.delete_files('previews', [check_exits[0]['preview']])
            del check_exits
            pid = self.mdb.insert_file_stream('previews', uid_str + ext, ext, r.content)
            if pid is not None:
                self.mdb.update('resources', {'identify': uid_str}, '$set', {'preview': pid})
        elif self.task_info['save_type'] == 'file' and len(r.content) > 1024:
            p_path = self.task_info['path'] + job['name'] + f"--#{uid_str}#" + ext
            p_path = file_path_fix(p_path)
            with open(p_path, 'wb') as code:
                code.write(r.content)
        else:
            self.damage[0] = self.damage[0] + 1
            self.logger.warning('preview_none')
        del r, img_url

    @staticmethod
    def download(url):
        try:
            return requests.get(url)
        except Exception as e:
            pass

    def ref(self, urls, cur):
        cr = cur
        rs = self.download(urls[cr])
        try:
            if rs is None and cr == len(urls) - 1:
                return None, cr
            elif rs is None and cur < len(urls) - 1:
                cr = cr + 1
                return self.ref(urls, cr)
            elif cur < len(urls) - 1 and len(rs.content) <= 1024:
                cr = cr + 1
                return self.ref(urls, cr)
        except Exception as e:
            print(e)
            return self.ref(urls, cr)
        return rs, cr

    # 根据网盘id下载种子线程配置
    def torrent_claw(self):
        res = self.mdb.find_by_kvpair('logs', {})[0]
        t_cur_index = int(res['t_cur_index'])
        self.progress = t_cur_index
        self.total_count = self.mdb.find_count('resources', {})
        queue = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'ASC']], limit=100,
                                        skip=t_cur_index)
        if len(queue) == 0:
            return
        total = len(queue) + t_cur_index
        workers = threadpool.makeRequests(self.get_torrent, queue)
        [self.t_pool.putRequest(req, block=False) for req in workers]
        self.t_pool.wait()
        del queue, workers, res
        self.mdb.update('logs', {}, '$set', {'t_cur_index': total})
        self.torrent_claw()

    # 根据网盘id下载种子线程worker
    def get_torrent(self, job):
        if job['identify'] == "":
            self.damage[0] = self.damage[0] + 1
            self.logger.warning('torrent-identify_miss')
            return None
        try:
            res = requests.request('POST', self.task_info['tor_server'],
                                   data={'code': job['identify'], "action": "download"})
        except:
            self.logger.warning('torrent-request_error')
            self.damage[0] = self.damage[0] + 1
            return None
        if res.url == "http://www.mimima.com/error.php":
            self.damage[0] = self.damage[0] + 1
            self.logger.warning('torrent-torrent_miss')
            self.mdb.delete('resources', {'url': job['url']})
            self.mdb.update('logs', {}, '$inc', {'total': -1})
            return None
        else:
            if self.task_info['save_type'] == 'db':
                check_exits = self.mdb.find_by_kvpair('resources', {'identify': job['identify']})
                if len(check_exits) > 0:
                    if check_exits[0]['torrent'] != "":
                        self.mdb.delete_files('torrents', [check_exits[0]['torrent']])
                if len(res.content) < 10:
                    self.damage[0] = self.damage[0] + 1
                    self.logger.warning('torrent-error_return')
                    return None
                tid = self.mdb.insert_file_stream('torrents', job['identify'] + '.torrent', '.torrent', res.content)
                self.mdb.update('resources', {'identify': job['identify']}, '$set', {'torrent': tid})
            else:
                t_path = self.task_info['path'] + f"#{job['identify']}#.torrent"
                t_path = file_path_fix(t_path)
                with open(t_path, 'wb') as code:
                    code.write(res.content)
        self.progress = self.progress + 1
        percent = self.progress / self.total_count * 100
        sys.stdout.write(
            "\r" + f"已下载:{self.progress}/{str(round(percent,2))}%, Missed:{self.damage[0]}, UniqueId:{job['identify']}")

    # 尝试下载丢失的连接
    def claw_miss_identify(self, limit=None):
        self.total_count = 0
        self.damage[0] = 0
        self.progress = 0
        res = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'ASC']], limit=limit)
        miss = []
        for each in res:
            if each['identify'] == '':
                self.total_count = self.total_count + 1
                miss.append(each)
        if len(miss) == 0:
            return
        workers = threadpool.makeRequests(self.get_unique, miss)
        [self.t_pool.putRequest(req, block=False) for req in workers]
        self.t_pool.wait()

    # 尝试下载丢失的缩略图
    def claw_miss_previews(self, limit=None):
        self.total_count = 0
        self.damage[0] = 0
        res = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'DSC']], limit=limit)
        miss = []
        for each in res:
            if each['preview'] == '':
                self.total_count = self.total_count + 1
                miss.append(each)
        if len(miss) == 0:
            return
        workers = threadpool.makeRequests(self.get_image_url, miss)
        [self.t_pool.putRequest(req, block=False) for req in workers]
        self.t_pool.wait()

    # 尝试下载丢失的种子
    def claw_miss_torrent(self, limit=None):
        self.total_count = 0
        self.damage[0] = 0
        res = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'ASC']], limit=limit)
        miss = []
        for each in res:
            if each['torrent'] == '' and each['identify'] != '':
                self.total_count = self.total_count + 1
                miss.append(each)
        if len(miss) == 0:
            return
        workers = threadpool.makeRequests(self.get_torrent, miss)
        [self.t_pool.putRequest(req, block=False) for req in workers]
        self.t_pool.wait()

    # 清理丢失的帖子
    def clear_damage_identify(self, limit=None):
        self.total_count = 0
        res = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'ASC']], limit=limit)
        miss = []
        for each in res:
            if each['identify'] == '':
                miss.append(each)
        for each in miss:
            self.mdb.delete('resources', {'_id': each['_id']})
            self.mdb.update('logs', {}, '$inc', {'total': -1})
        dep = []
        for each in res:
            if not each['identify'] in dep:
                dep.append(each['identify'])
            else:
                self.mdb.delete('resources', {'_id': each['_id']})
                self.mdb.update('logs', {}, '$inc', {'total': -1})

    def clear_duplicate_image(self, limit=None):
        pes = self.mdb.find_by_kvpair('previews.files', {}, sort_list=[['timestamp', 'ASC']], limit=limit)
        res = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'ASC']], limit=limit)
        dep = []
        det = []
        for each in res:
            dep.append(each['preview'])
        for each in pes:
            det.append(str(each['_id']))
        diff = list(set(det) - set(dep))
        self.mdb.delete_files('previews', diff)

    # 清理丢失的种子
    def clear_damage_torrents(self, limit=None):
        tes = self.mdb.find_by_kvpair('torrents.files', {}, sort_list=[['timestamp', 'ASC']], limit=limit)
        res = self.mdb.find_by_kvpair('resources', {}, sort_list=[['timestamp', 'ASC']], limit=limit)
        miss = []
        for each in res:
            if each['torrent'] == '':
                miss.append(each)
        for each in miss:
            self.mdb.delete('resources', {'_id': each['_id']})
            self.mdb.update('logs', {}, '$inc', {'total': -1})
        dep = []
        det = []
        for each in res:
            dep.append(each['torrent'])
        for each in tes:
            det.append(str(each['_id']))
        diff = list(set(det) - set(dep))
        self.mdb.delete_files('torrents', diff)

    # 清理数据库
    def clear_db(self):
        self.mdb.multi_update('resources', {}, '$set', {'preview': '', 'torrent': '', 'img_url': ''})
        self.mdb.drop_col('previews.chunks')
        self.mdb.drop_col('previews.files')
        self.mdb.drop_col('torrents.chunks')
        self.mdb.drop_col('torrents.files')
        self.mdb.update('logs', {}, '$set', {'p_cur_index': 0})
        self.mdb.update('logs', {}, '$set', {'t_cur_index': 0})

    def task_dispatch(self):
        # self.page_claw()
        # self.clear_db()
        # self.identify_claw()
        # for x in range(5):
        #     self.claw_miss_identify()
        # self.clear_damage_identify()
        self.previews_claw()
        # for x in range(2):
        #     self.claw_miss_previews()
        # self.clear_duplicate_image()
        # if input("\nPlease shutdown proxy and start torrents claw Y/N：").upper() == "Y":
        #     self.torrent_claw()
        # for x in range(5):
        #     self.claw_miss_torrent()
        # self.clear_damage_torrents()


task = {
    'server': "http://www.ac168.info/bt/",  # 论坛服务
    'target': "http://www.ac168.info/bt/thread.php?fid=5&page={0}",  # 论坛页面分页url
    'tor_server': 'http://www.jandown.com/fetch.php',  # 网盘地址
    'save_type': 'db',  # 文件存储方式
    'path': "G:\\迅雷下载\\Torrents\\",  # 文件存储路径
    'list_identify': 'class',  # 帖子列表爬取标识
    'list_type': 'tr',
    'list_identify_text': 'tr3 t_one',
    'list_element': 'dd',
    'content_identify': 'class',  # 帖子内容爬取标识
    'content_type': 'div',
    'content_identify_text': 'tpc_content'
}
s = TorrentsClaw(task)
s.task_dispatch()
