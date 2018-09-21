import pymysql
import datetime

class MySQLOP:
    def __init__(self, host, port, db, user, pwd):
        self.host = host if host else '127.0.0.1'
        self.port = port if port else 3306
        self.db = db if db else "mysql"
        self.user = user if user else "root"
        self.pwd = pwd if pwd else "qnyh1991"
        self.db = pymysql.connect(self.host, self.user, self.pwd, self.db, self.port)

    def insert(self, table, para):
        sql = "INSERT INTO " + table + " ("
        for key in para.keys():
            sql = sql + key + ","
        sql = sql[0: int(len(sql) - 1)] + ") VALUES ("
        for i in range(len(para.keys())):
            sql = sql + ":" + str(i + 1) + ","
        sql = sql[0: len(sql) - 1] + ")"
        cursor = self.db.cursor()
        cursor.execute(sql, list(para.values()))
        self.db.commit()
        cursor.close()

    def query(self, table, cols, filters=None, filtersrelations=None, is_distinct=False):
        if is_distinct:
            sql = "SELECT DISTINCT "
        else:
            sql = "SELECT "
        for key in cols:
            sql = sql + key + ","
        sql = sql[0: int(len(sql) - 1)] + " FROM " + table
        if len(filters.keys()) > 0:
            sql = sql + " WHERE "
            i = 0
            for key in filters.keys():
                if isinstance(filters[key], str):
                    sql = sql + key + " = '" + filters[key] + "'"
                elif filters[key] is None:
                    sql = sql + key + " IS " + "NULL"
                elif isinstance(filters[key], list):
                    sql = sql + key + " IS NOT NULL"
                else:
                    sql = sql + key + " = " + str(filters[key])
                if i < len(filters.keys()) - 1:
                    if len(filtersrelations) != 0:
                        sql = sql + " " + filtersrelations[i] + " "
                    i = i + 1
        else:
            pass
        cursor = self.db.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def update(self, table, paras, filters=None, filtersrelations=None, content=None):
        sql = "UPDATE " + table + " SET "
        for key in paras:
            sql = sql + key + "="
            if isinstance(paras[key], (str, datetime.datetime)) and not str(paras[key]).__contains__(":"):
                sql = sql + "'" + paras[key] + "',"
            elif isinstance(paras[key], str) and str(paras[key]).__contains__(":"):
                if str(paras[key]).index(':') == 0:
                    sql = sql + str(paras[key]) + ","
                else:
                    sql = sql + "'" + paras[key] + "',"
            else:
                sql = sql + str(paras[key]) + ","
        sql = sql.rstrip(',')
        if len(filters.keys()) > 0:
            sql = sql + " WHERE "
            i = 0
            for key in filters.keys():
                if isinstance(filters[key], str):
                    sql = sql + key + " = '" + filters[key] + "'"
                else:
                    sql = sql + key + " = " + filters[key]
                if i < len(filters.keys()) - 1:
                    sql = sql + filtersrelations[i]
                    i = i + 1
        cursor = self.db.cursor()
        cursor.execute(sql, content)
        self.db.commit()
        cursor.close()

    def execute_query_by_sql(self, sql):
        cursor = self.db.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def execute_by_sql(self, sql):
        cursor = self.db.cursor()
        cursor.execute(sql)
        self.db.commit()
        cursor.close()

    def close(self):
        self.db.close()
