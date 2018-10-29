from NetWorkSpider.MultiThreadNetWorm.SMTHtmlRequest import DownloadQueue

#
# a = "http://www.stats.gov.cn/tjsj/tjbz/xzqhdm/201703/t20170310_1471429.html"
# ad = AdministrationClaw(a, a)
# ad.claw("GET", a)
# ad.admin_tree()

task1 = {
    'server': "http://www.biqugeg.com",
    'target': "http://www.biqugeg.com/48_48938/",
    'path': "F:\\迅雷下载\\桃运大相师.txt",
    'bookname': "桃运大相师",
    'list_identify': 'class',
    'list_type': 'div',
    'list_identify_text': 'listmain',
    'list_element': 'dd',
    'content_identify': 'id',
    'content_type': 'div',
    'content_identify_text': 'content'
}
task2 = {
    'server': "http://www.biqugexsw.com",
    'target': "http://www.biqugexsw.com/4_4495/",
    'path': "F:\\迅雷下载\\女神的布衣兵王.txt",
    'bookname': "女神的布衣兵王",
    'list_identify': 'class',
    'list_type': 'div',
    'list_identify_text': 'listmain',
    'list_element': 'dd',
    'content_identify': 'id',
    'content_type': 'div',
    'content_identify_text': 'content'
}
task3 = {
    'server': "https://www.qu.la",
    'target': "https://www.qu.la/book/85467/",
    'path': "F:\\迅雷下载\\重生之都市仙尊.txt",
    'bookname': "重生之都市仙尊",
    'list_identify': 'id',
    'list_type': 'div',
    'list_identify_text': 'list',
    'list_element': 'dd',
    'content_identify': 'id',
    'content_type': 'div',
    'content_identify_text': 'content'
}
s = DownloadQueue(task1)
s1 = DownloadQueue(task2)
s.download()
# s1.download()
