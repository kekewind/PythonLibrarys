import uuid

from bs4 import BeautifulSoup

from BasicLibrarys.Common import HttpRequestBase, OracleDBOP


class AdministrationClaw:
    def __init__(self, server, target):
        self.server = server
        self.target = target
        self.db = OracleDBOP.OracleOP("localhost", 1521, "OCDB", "SASYS", "123456")

    def claw(self, method, url):
        http = HttpRequestBase.HttpRequestBase()
        html = http.urllib3_request(method, url)
        self.AdminListGet(html)

    def AdminListGet(self, html):
        besdl = BeautifulSoup(html, "html.parser")
        texts = besdl.find_all("p", class_="MsoNormal")
        povid = ""
        povname = ""
        cityid = ""
        cityname = ""
        for each in texts:
            beslist = BeautifulSoup(str(each), "html.parser")
            lista = beslist.find_all("b")
            list_span = beslist.find_all("span")
            if len(lista) == 2:
                pov = BeautifulSoup(str(lista[0]), "html.parser")
                povcode = beslist.find_all("span")
                code = povcode[0].contents[0]
                name = povcode[2].contents[0]
                povid = str(uuid.uuid1())
                povname = name
                para = {
                    "POVID": povid,
                    "POVNAME": name,
                    "CODE": code
                }
                self.db.insert("SA_DISTRICTINDEX", para)
            elif len(list_span) == 4:
                code = list_span[1].contents[0]
                text = str(list_span[3].contents[0]).strip()
                if text is None:
                    continue
                if str(list_span[1].contents[1]).count('\xa0') == 8:
                    cityid = str(uuid.uuid1())
                    cityname = text
                    para = {
                        "CITYID": cityid,
                        "POVID": povid,
                        "CITYNAME": cityname,
                        "POVNAME": name,
                        "CODE": code
                    }
                    self.db.insert("SA_DISTRICTINDEX", para)
                elif str(list_span[1].contents[1]).count('\xa0') == 4:
                    countyid = str(uuid.uuid1())
                    countyname = text
                    para = {
                        "COUNTYID": countyid,
                        "CITYID": cityid,
                        "POVID": povid,
                        "COUNTYNAME": countyname,
                        "CITYNAME": cityname,
                        "POVNAME": name,
                        "CODE": code
                    }
                    self.db.insert("SA_DISTRICTINDEX", para)

    def admin_tree(self):
        tree = {}
        list_pov = self.db.query("SA_DISTRICTINDEX", ["POVNAME", "POVID", "CODE", "CX", "CY"],
                                 {"CITYID": None, "COUNTYID": None}, ["AND"])
        for each in list_pov:
            tree[each[0]] = {
                "id": each[1],
                "code": each[2],
                "x": each[3],
                "y": each[4],
                "city": {}
            }
            list_city = self.db.query("SA_DISTRICTINDEX", ["CITYNAME", "CITYID", "CODE", "CX", "CY"],
                                      {"POVID": each[1], "COUNTYID": None}, ["AND"])
            if list_city == None:
                continue
            for city in list_city:
                if city[1] is None or city[0] is None:
                    continue
                tree[each[0]]["city"][city[0]] = {
                    "id": city[1],
                    "code": city[2],
                    "x": city[3],
                    "y": city[4],
                    "county": {}
                }
                if city[1] is None:
                    continue
                list_county = self.db.query("SA_DISTRICTINDEX", ["COUNTYNAME", "COUNTYID", "CODE", "CX", "CY"],
                                            {"POVID": each[1], "CITYID": city[1], "COUNTYID": []}, ["AND", "AND"])
                for county in list_county:
                    tree[each[0]]["city"][city[0]]["county"][county[0]] = {
                        "id": county[1],
                        "code": county[2],
                        "x": county[3],
                        "y": county[4]
                    }
        return tree
