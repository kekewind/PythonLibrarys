import csv
import gc
import json
import os
import sys
import time
import uuid
from datetime import datetime

import psutil
from elasticsearch import Elasticsearch, helpers, exceptions as esex
from elasticsearch_dsl import AttrDict
from redis import Redis, exceptions as redisex

from BasicLibrarys.Common.FileUtils import FileUtils
from BigDataAnalysis.ElasticSearch_OP.ESSearcher import ESSearcher
#from CrossPlatform.PythonXCSharp.ParallelForTest import MultiThreadUtils


class ESOperation:
    def __init__(self, mapping_dict=None):
        self.host = 'localhost'
        self.port = 9200
        self.es_client = Elasticsearch([{"host": self.host, "port": self.port}])
        self.csv_path = None
        self.__indices = self.es_client.indices
        self.__cat = self.es_client.cat
        self.__cluster = self.es_client.cluster
        self.__nodes = self.es_client.nodes
        self.map_dict = mapping_dict
        self.__doc_type = ""
        self.__index_name = ""
        self.rdb = Redis(host="localhost", port=6379, db=0)
        self.rdb_log = Redis(host="localhost", port=6379, db=1)
        self.rdb_cache = Redis(host="localhost", port=6379, db=2)
        self.timer = time
        print("Establish connection to ElasticSearch cluster success")

    def set_mapping(self, mapping_dict):
        self.map_dict = mapping_dict
        return True

    def set_index_mapping(self, index_name=None, doc_type=None):
        if doc_type is None:
            doc_type = 'test'
        if index_name is None:
            index_name = uuid.uuid4()
        if self.__indices.exists(index_name):
            self.__indices.delete(index_name)
        if self.map_dict is None:
            create_index = self.__indices.create(index=index_name, ignore=400)
            mapping_index = {'acknowledged': True}
        else:
            self.__doc_type = doc_type
            self.__index_name = index_name
            mapping = {doc_type: {'properties': {}}}
            properties = mapping[doc_type]['properties']
            for key in self.map_dict.keys():
                if str(self.map_dict[key]).__contains__(','):
                    f_type, index = str(self.map_dict[key]).split(',')
                    properties[key] = {'type': f_type, 'index': index}
                else:
                    properties[key] = {'type': self.map_dict[key]}
            properties['location'] = {'type': 'geo_point'}
            create_index = self.__indices.create(index=index_name, ignore=400)
            mapping_index = self.__indices.put_mapping(index=index_name, doc_type=doc_type, body=mapping)
        if create_index["acknowledged"] is not True or mapping_index["acknowledged"] is not True:
            print("Index and Mapping creation failed")
            return False
        else:
            print("Index and Mapping creation success")
            return True

    def csv_to_es(self, csv_path, xy_index):
        print("ElasticSearch cluster health status %s" % self.__cluster.health()['status'])
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                csv_file = csv.reader(f)
                for raw in csv_file:
                    del raw
                    break
                if csv_file is not None:
                    actions = []
                    index = 0
                    for line in csv_file:
                        action = self.__line_to_dic(line, xy_index=xy_index)
                        actions.append(action)
                        del action, line
                        if len(actions) == 100000:
                            for success, info in helpers.parallel_bulk(self.es_client, actions, chunk_size=100000,
                                                                       queue_size=5, thread_count=5,
                                                                       **{"request_timeout": 1000}):
                                if not success:
                                    print(info)
                                del success, info
                            actions.clear()
                            index = index + 1
                            print("100000 records already insert into ElasticSearch index:%d" % index)
                            gc.collect()
                    for success, info in helpers.parallel_bulk(self.es_client, actions, chunk_size=100000,
                                                               queue_size=5, thread_count=5):
                        if not success:
                            print(info)
                        del success, info
                    print("%d records already insert into ElasticSearch" % len(actions))
                    actions.clear()
                    gc.collect()
            self.__indices.refresh()
            print("ES indices parallel bulk complete")
            print("total index documents count %d" % self.es_client.count(self.__index_name)['count'])
        except [IOError, helpers.BulkIndexError]:
            print("file read error!")

    def json_to_es(self, json_path):
        print("ElasticSearch cluster health status %s" % self.__cluster.health()['status'])
        # file_list = FileUtils.file_scan_by_time(json_path)
        # while len(file_list) > 0:
        #     json_file = json_path + file_list.pop()
        #     with open(json_file, 'r', encoding='utf-8') as f:
        #         chunk = f.read()
        #         json_dic = json.loads(chunk)
        #         mu = MultiThreadUtils()
        #         mu.run(0, len(json_dic), lambda i: self.action_process(json_dic[i]))
        #         for success, info in helpers.parallel_bulk(self.es_client, json_dic, chunk_size=100000, queue_size=5,
        #                                                    thread_count=5):
        #             if not success:
        #                 print(info)
        #         print('One json file inserted into ElasticSearch')
        #         f.flush()
        #         f.close()
        #         del json_dic, chunk
        #     del f
        #     gc.collect()

    def action_process(self, action):
        action['_op_type'] = "index"
        action['_index'] = self.__index_name
        action['_type'] = self.__doc_type

    def __line_to_dic(self, line, xy_index=None):
        body_dic = {'_source': {}}
        index = 0
        for key in self.map_dict.keys():
            if self.map_dict[key] == "double":
                if line[index] == "null":
                    line[index] = 0.0
                body_dic['_source'][key] = float(line[index])
            elif self.map_dict[key] == "geo_point" and len(xy_index) == 2:
                body_dic['_source'][key] = {'lat': line[xy_index[1]],
                                            'lon': str(((float(line[xy_index[0]]) + 180) % 360) - 180)}
            elif self.map_dict[key] == "text":
                if line[index] == "null":
                    line[index] = ""
                body_dic['_source'][key] = str(line[index])
            elif self.map_dict[key] == "integer" or self.map_dict[key] == "long":
                if line[index] == "null":
                    line[index] = '0'
                body_dic['_source'][key] = int(line[index])
            elif self.map_dict[key] == "float":
                try:
                    if line[index] == "null":
                        line[index] = 0.0
                    body_dic['_source'][key] = datetime.strptime(line[index], "%Y-%m-%dT%H:%M:%SZ").timestamp() * 1000
                except:
                    body_dic['_source'][key] = 0.0
            elif self.map_dict[key] == "date":
                if line[index] == "null":
                    line[index] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                elif line[index].__contains__("T"):
                    body_dic['_source'][key] = line[index]
                else:
                    body_dic['_source'][key] = datetime.strptime(line[index], "%Y-%m-%d %H:%M:%S").strftime(
                        "%Y-%m-%dT%H:%M:%SZ")
            else:
                body_dic['_source'][key] = str(line[index])
            index = index + 1
        body_dic['_op_type'] = "index"
        body_dic['_index'] = self.__index_name
        body_dic['_type'] = self.__doc_type
        return body_dic

    def es_data_export(self, source_host, source_index, source_doc_type, path, sort_obj=None):
        if self.rdb_log.get('current_file_count') is None:
            self.rdb_log.flushdb()
            self.rdb.flushdb()
            self.rdb_log.set('current_file_count', 0)
        self.host = source_host
        self.es_client = Elasticsearch([{"host": self.host, "port": self.port}])
        print("Change ElasticSearch Server to %s" % self.host)
        searcher = ESSearcher(self.es_client)
        result = searcher.bool_search(source_index, source_doc_type, {'match_all': {}},
                                      sort=sort_obj)
        total = result['hits']['total']
        old_count = int(self.rdb_log.get('current_file_count'))
        count = old_count * 100000
        if (total - count) < 100000 and self.rdb_log.get('iscache'):
            self.__redis_cache_writer(path)
            return
        if count != 0:
            result = self.__get_recent_index(path, source_index, source_doc_type, 'mmsi', sort_obj)
        meta = (source_host, source_index, source_doc_type, path)
        self.__recursion_scroll_search(meta, count, result, total)
        self.rdb_log.flushdb()
        self.__redis_cache_writer(path)
        gc.collect()

    def __get_recent_index(self, path, source_index, source_doc_type, key, sort_obj):
        file_list = FileUtils.file_scan_by_time(path)
        file_p = file_list.pop()
        with open(path + file_p, 'r', encoding='utf-8') as f:
            chunk = f.read()
            list_chunk = json.loads(chunk)
            current_time = list_chunk[99999]['receive_time']
            key_v = list_chunk[99999][key]
            f.flush()
            f.close()
            del chunk, list_chunk
            gc.collect()
        searcher = ESSearcher(self.es_client)
        result = searcher.bool_search(source_index, source_doc_type, {'match_all': {}},
                                      filter_list=[{"range": {"receive_time": {"gte": current_time}}}],
                                      sort=sort_obj)
        cache_index = 0
        count_len = len(result['hits']['hits'])
        scroll_id = result['_scroll_id']
        for i in range(count_len):
            if key_v == result['hits']['hits'][i]['_source'][key]:
                cache_index = i
                break
        for i in range(cache_index + 1, count_len):
            if isinstance(result['hits']['hits'][i]['_source'], AttrDict):
                tmp = json.dumps(result['hits']['hits'][i]['_source'].to_dict()) + ','
            else:
                tmp = json.dumps(result['hits']['hits'][i]['_source']) + ','
            self.rdb_cache.set(i, tmp)
            del tmp
        del result
        self.rdb_log.set('iscache', True)
        gc.collect()
        return self.es_client.scroll(scroll='3m', scroll_id=scroll_id)

    def __recursion_scroll_search(self, meta, count, result, total):
        if psutil.virtual_memory().percent > 90:
            return
        indexes = 0
        tmp_res = result
        del result
        for cursor in range(int(total / 100000) + 1):
            for i in range(10):
                scroll_id = tmp_res['_scroll_id']
                for each in tmp_res['hits']['hits']:
                    if isinstance(each['_source'], AttrDict):
                        tmp = json.dumps(each['_source'].to_dict()) + ','
                    else:
                        tmp = json.dumps(each['_source']) + ','
                    self.rdb.set(indexes, tmp)
                    indexes = indexes + 1
                    count = count + 1
                    del each, tmp
                try:
                    self.rdb.bgsave()
                except redisex.ResponseError:
                    pass
                sys.stdout.write('\r' + 'Already download %d,time_cost:%s' % (count, self.timer.process_time()))
                if count % 100000 == 0:
                    last_str = str(self.rdb.get(indexes - 1), encoding='utf-8')
                    new_last_str = last_str[:last_str.rfind(',')]
                    self.rdb.set(indexes - 1, new_last_str)
                    del last_str, new_last_str
                    self.__json_file_writer(meta[3], indexes)
                if indexes % 100000 == 0:
                    indexes = 0
                tmp_res = None
                time.sleep(1)
                gc.collect()
                try:
                    tmp_res = self.es_client.scroll(scroll='3m', scroll_id=scroll_id)
                    del scroll_id
                    gc.collect()
                    if tmp_res is None:
                        self.es_data_export(meta[0], meta[1], meta[2], meta[3])
                except (esex.NotFoundError, redisex.TimeoutError, esex.ConnectionTimeout):
                    self.es_data_export(meta[0], meta[1], meta[2], meta[3])

    def __json_file_writer(self, path, indexes):
        if not os.path.isdir(path):
            os.mkdir(path)
        file = str(path + str(uuid.uuid4()) + '.json')
        with open(file, 'ab+') as f:
            f.write(bytes('[', encoding='utf-8'))
            for i in range(indexes):
                f.write(self.rdb.get(i))
                f.write(bytes('\n', encoding='utf-8'))
                f.flush()
            f.write(bytes(']', encoding='utf-8'))
            f.flush()
            f.close()
        self.rdb.flushdb()
        self.rdb_log.set('current_file_count', int(self.rdb_log.get('current_file_count')) + 1)
        print("\nWrite 100000 part json file %s" % file)
        del f, file
        gc.collect()

    def __redis_cache_writer(self, path):
        if not os.path.isdir(path):
            os.mkdir(path)
        file = str(path + str(uuid.uuid4()) + '.json')
        keys_db = self.rdb.keys()
        keys_cache = self.rdb_cache.keys()
        with open(file, 'ab+') as f:
            f.write(bytes('[', encoding='utf-8'))
            for i in keys_db:
                f.write(self.rdb.get(str(i, encoding='utf-8')))
                f.write(bytes('\n', encoding='utf-8'))
                f.flush()
            for i in keys_cache:
                f.write(self.rdb_cache.get(str(i, encoding='utf-8')))
                f.write(bytes('\n', encoding='utf-8'))
                f.flush()
            f.write(bytes(']', encoding='utf-8'))
            f.flush()
            f.close()
        self.rdb.flushdb()
        self.rdb_cache.flushdb()


flights_mappings = {"flight_id": "keyword", "direction": "short", "planetype": "text", "ident": "keyword",
                    "icon": "text", "origin": "keyword", "originLabel": "keyword", "destination": "keyword",
                    "destinationLabel": "keyword", "prominence": "double", "altitude": "double",
                    "groundspeed": "double", "projected": "double", "x": "double", "y": "double", "z": "double",
                    "time-ms": "double", "datetime": "date", "location": "geo_point"}
shippos_mapping = {"mmsi": "long", "pos_time": "float", "Longitude": "double", "Latitude": "double", "Speed": "double",
                   "Course": "double", "Heading": "double", "Rot": "double", "nav_status_id": "text",
                   "Accuracy": "text", "class_type": "text", "ais_type": "text", "ais_source_id": "text",
                   "receive_time": "date", "insert_time": "date", "pin.location": "geo_point"}
es_f = ESOperation(flights_mappings)
es_s = ESOperation(shippos_mapping)
#es_f.set_index_mapping('flights', 'flight_utc')
# es_s.set_index_mapping('shippos', 'shippos')
es_f.es_data_export('54.223.164.155', 'flights', 'flight_utc', 'f:\\',
                    sort_obj={"time-ms": {"order": "ASC"}})
# es.es_data_export('54.223.164.155', 'shippos', 'shippos', 'f:\\Ships_Data\\',
#                   sort_obj={"receive_time": {"order": "ASC"}})
es_f.csv_to_es('H:\\Big_Data\\flights.csv', [13, 14])
# es_s.csv_to_es('H:\\Big_Data\\shippos.csv', [2, 3])
