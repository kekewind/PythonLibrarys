from NetWorkSpider.MultiThreadNetWorm.SMTHtmlRequest import DownloadQueue

#
# a = "http://www.stats.gov.cn/tjsj/tjbz/xzqhdm/201703/t20170310_1471429.html"
# ad = AdministrationClaw(a, a)
# ad.claw("GET", a)
# ad.admin_tree()

task1 = {
    'server': "http://www.biqugeg.com",
    'target': "http://www.biqugeg.com/48_48938/",
    'path': "F:\\迅雷下载\\ab.txt",
    'bookname': "ab",
    'list_identify': 'class',
    'list_type': 'div',
    'list_identify_text': 'listmain',
    'list_element': 'dd',
    'content_identify': 'id',
    'content_type': 'div',
    'content_identify_text': 'content'
}
s = DownloadQueue(task1)
s.download()
# s1.download()

