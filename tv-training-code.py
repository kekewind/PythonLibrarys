import html
import json
import os

import requests

from BasicLibrarys.Common.HttpRequestBase import HttpRequestBase

url = "http://photo.weibo.com/photos/get_all"

querystring = {"uid": "1745375470", "album_id": "3555513674493708", "type": "3", "page": "10", "count": "32",
               "__rnd": "1560428517069"}

headers = {
    'Accept': "*/*",
    'Accept-Encoding': "gzip, deflate",
    'Accept-Language': "zh-CN,zh;q=0.9",
    'Connection': "keep-alive",
    'Content-Type': "application/x-www-form-urlencoded",
    'Cookie': "UOR=www.cocoachina.com,widget.weibo.com,www.csdn.net; _s_tentry=passport.weibo.com; Apache=921615183805.613.1560426167288; SINAGLOBAL=921615183805.613.1560426167288; ULV=1560426167890:1:1:1:921615183805.613.1560426167288:; login_sid_t=9d0209b74b5fcab2ca0a0e98e6fc818b; cross_origin_proto=SSL; SCF=AnObj5_5b0kjGH9_ftZ7iC8i9BI7LwOvcijHoGI5NOI0L_uaq0s9LXpxhMgZR8iP72C8WoiBxBwmbasxt4gsde8.; SUB=_2A25wBkgfDeRhGeNG71IX-SfEzjmIHXVTcj7XrDV8PUNbmtAKLRahkW9NS0FbdEI4IfSo8AtaUoh4vUDoXek60aVW; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWhEgiPHpRhH-EC_v_d140Q5JpX5K2hUgL.Fo-RSh5c1K.RSK-2dJLoI7U-HGyRCgvV; SUHB=0N21mWtsp11h12; ALF=1561031376; SSOLoginState=1560426576; un=18801056109; __guid=88633178.699397267338460200.1560426981361.1553; monitor_count=8; webim_unReadCount=%7B%22time%22%3A1560428515885%2C%22dm_pub_total%22%3A0%2C%22chat_group_pc%22%3A0%2C%22allcountNum%22%3A4%2C%22msgbox%22%3A0%7D",
    'DNT': "1",
    'Host': "photo.weibo.com",
    'Referer': "http://photo.weibo.com/1745375470/talbum/index",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
    'X-Requested-With': "XMLHttpRequest",
    'Cache-Control': "no-cache",
    'Postman-Token': "8ec75618-fde0-412b-b169-6219e83dbe6a,c47a8e40-1cdf-4f0b-bc21-4335945fc314",
    'cache-control': "no-cache"
}


res = []
for i in range(50):
    querystring['page'] = i + 1
    print(i)
    response = requests.request("GET", url, headers=headers, params=querystring)
    resk = json.loads(html.unescape(response.content.decode(encoding='utf-8')))
    for each in resk['data']['photo_list']:
        res.append(each["pic_host"] + "/" + "mw690/" + each['pic_name'])

with open('s.json', 'a') as f:
    f.write(json.dumps(res))


def file_scan_by_time(directory):
    if not str(directory).endswith('\\'):
        directory = directory + '\\'
    file_list = [(i, os.stat(directory + i).st_mtime) for i in os.listdir(directory)]
    file_list_asc = []
    for i in sorted(file_list, key=lambda x: x[1], reverse=True):
        file_list_asc.append(i[0])
    return file_list_asc


with open('s.json', 'r') as f:
    htt = HttpRequestBase()
    ws = f.read()
    s = json.loads(ws)
    c = []
    al = file_scan_by_time("f:\\aa.json")
    for each in s:
        index = -1
        for item in al:
            if each.__contains__(item):
                index = s.index(each)
        if index == -1:
            c.append(each)
        else:
            index = -1

    print(c)
