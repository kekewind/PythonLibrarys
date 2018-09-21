from elasticsearch_dsl import search, query


class ESSearcher:
    def __init__(self, client=None):
        self.host = 'localhost'
        self.port = 9200
        self.es_client = client
        self.csv_path = None
        self.__indices = self.es_client.indices
        self.__cat = self.es_client.cat
        self.__cluster = self.es_client.cluster
        self.__nodes = self.es_client.nodes

    def search_by_dict(self, search_dic, index, doc_type, ):
        search_obj = search.Search(using=self.es_client, index=index, doc_type=doc_type).from_dict(search_dic).params(
            scroll='1m')
        res = search_obj.execute()
        return res

    @staticmethod
    def query_builder(name_or_query, filter_dic=None):
        if filter_dic is not None:
            filter_obj = []
            for key in filter_dic.keys():
                if key == "range":
                    filter_obj.append(
                        {"range": {filter_dic[key][0]: {"from": filter_dic[key][1], "to": filter_dic[key][2]}}})
                elif key == "term":
                    filter_obj.append({"term": {filter_dic[key][0]: filter_dic[key][1]}})
                elif key == "geo_bounding_box":
                    filter_obj.append({"geo_bounding_box": {filter_dic[key][0]: filter_dic[key][1]}})
            return query.Q(name_or_query, filter=filter_obj[0])
        else:
            return query.Q(name_or_query)

    def bool_search(self, index, doc_type, match_dic, filter_list=None, sort=None, size=10000, f=0):
        search_obj = search.Search(using=self.es_client, index=index, doc_type=doc_type).params(scroll='1m').extra(
            from_=f, size=size)
        filter_obj = []
        if filter_list is not None:
            for each in filter_list:
                filter_obj.append(query.Q(each))
        query_obj = query.Q('bool', must=query.Q(match_dic), filter=filter_obj)
        if sort is not None:
            search_obj = search_obj.sort(sort)
        search_obj = search_obj.query(query_obj)
        res = search_obj.execute()
        return res

    def geo_hash_grid_search(self, meta, start_end_time, time_field, extent, geo_field, precision):
        search_obj = search.Search(using=self.es_client, index=meta[0], doc_type=meta[1]).params(scroll='1m')
        time_query = self.query_builder('constant_score', {"range": (time_field, start_end_time[0], start_end_time[1])})
        search_obj=search_obj.query(time_query)
