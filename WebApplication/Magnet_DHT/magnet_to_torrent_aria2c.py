#!usr/bin/python
# encoding=utf8

import json
from http.client import HTTPConnection

from WebApplication.Magnet_DHT.database import RedisClient

SAVE_PATH = "E:\\torrents\\"
STOP_TIMEOUT = 2
MAX_CONCURRENT = 16
MAX_MAGNETS = 256

ARIA2RPC_ADDR = "127.0.0.1"
ARIA2RPC_PORT = 6800

rd = RedisClient()


def get_magnets():
    """
    获取磁力链接
    """
    mgs = rd.get_magnets(MAX_MAGNETS)
    for m in mgs:
        # 解码成字符串
        yield m.decode()


def exec_rpc(magnet):
    """
    使用 rpc，减少线程资源占用，关于这部分的详细信息科参考
    https://aria2.github.io/manual/en/html/aria2c.html?highlight=enable%20rpc#aria2.addUri
    """
    conn = HTTPConnection(ARIA2RPC_ADDR, ARIA2RPC_PORT)
    req = {
        "jsonrpc": "2.0",
        "id": "magnet",
        "method": "aria2.addUri",
        "params": [
            [magnet],
            {
                "bt-stop-timeout": str(STOP_TIMEOUT),
                "max-concurrent-downloads": str(MAX_CONCURRENT),
                "listen-port": "6881",
                "dir": SAVE_PATH,
            },
        ],
    }
    conn.request(
        "POST",
        "/jsonrpc",
        json.dumps(req),
        {"Content-Type": "application/json"},
    )

    res = json.loads(conn.getresponse().read())
    if "error" in res:
        print("Aria2c replied with an error:", res["error"])


def magnet2torrent():
    """
    磁力转种子
    """

    for magnet in get_magnets():
        print(magnet)
        exec_rpc(magnet)


magnet2torrent()
