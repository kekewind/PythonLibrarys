import csv
import gc
import json
import os
import threading
import uuid


class FileUtils:
    def __init__(self):
        self.name = None
        self.args = []
        self.target_method = None
        self.cond = threading.Condition()
        self.__threads = {}

    class MultiThreadHandler(threading.Thread):
        def __init__(self, cond, name, out):
            self.out = out
            super(self.__class__, self).__init__()
            self.cond = cond
            self.name = name

        def run(self):
            while len(self.out.args) > 1:
                self.cond.acquire()
                if len(self.out.args) > 1:
                    arg = self.out.args.pop()
                self.cond.release()
                self.out.target_method(arg)
                del arg

    @staticmethod
    def file_scan_by_time(directory):
        if not str(directory).endswith('\\'):
            directory = directory + '\\'
        file_list = [(i, os.stat(directory + i).st_mtime) for i in os.listdir(directory)]
        file_list_asc = []
        for i in sorted(file_list, key=lambda x: x[1], reverse=True):
            file_list_asc.append(i[0])
        return file_list_asc

    @staticmethod
    def merge_json(json_dir, target_file):
        if not str(json_dir).endswith('\\'):
            json_dir = json_dir + '\\'
        file_list_asc = FileUtils.file_scan_by_time(json_dir)
        index = 0
        while True:
            target = target_file + str(uuid.uuid4()) + '.json'
            with open(target, 'a+', encoding='utf-8') as f:
                f.write('[')
                while len(file_list_asc) > 0:
                    tmp_file = file_list_asc.pop()
                    with open(json_dir + tmp_file, 'r+', encoding='utf-8') as tmp_f:
                        chunk = tmp_f.read()
                        if index == 9:
                            chunk = chunk.replace('[', '').replace(']', '')
                        else:
                            chunk = chunk.replace('[', '').replace(']', '') + ','
                        f.write(chunk)
                        del chunk
                        tmp_f.flush()
                        tmp_f.close()
                        index = index + 1
                        if index >= 10:
                            index = 0
                            break
                    del tmp_f
                f.write(']')
                f.flush()
                f.close()
                print('1000000 json file has being written')
            if len(file_list_asc) == 0:
                break

    @staticmethod
    def json_file_fix(json_dir):
        if not str(json_dir).endswith('\\'):
            json_dir = json_dir + '\\'
        file_list_asc = FileUtils.file_scan_by_time(json_dir)
        while len(file_list_asc) > 0:
            tmp_file = file_list_asc.pop()
            with open("f:\\resource\\" + str(uuid.uuid4()) + ".json", 'a+', encoding='utf-8') as ff:
                with open(json_dir + tmp_file, 'r') as f:
                    while f.readable():
                        line = f.readline()
                        if line == "":
                            break
                        if line.__contains__('}},'):
                            ff.write(str(line))
                        else:
                            print(tmp_file + "rep")
                            line = line.replace('}}', '}},')
                            ff.write(str(line))
                        del line
                    f.flush()
                    f.close()
                ff.flush()
                ff.close()

    @staticmethod
    def json_to_csv(json_dir, csv_file):
        if not str(json_dir).endswith('\\'):
            json_dir = json_dir + '\\'
        file_list_asc = FileUtils.file_scan_by_time(json_dir)
        with open(csv_file, 'w', encoding='utf-8', newline='') as csv_f:
            with open(json_dir + file_list_asc[0], 'r') as tmp_f:
                line = tmp_f.readline().replace("\\u0000", "0.0")
                tmp = json.loads(line[1:line.__len__() - 2])
                titles = [field for field in tmp.keys()]
                tmp_f.flush()
                tmp_f.close()
            writer = csv.DictWriter(csv_f, fieldnames=titles[0:len(titles) - 1])
            writer.writeheader()
            csv_f.flush()
            del tmp_f
            gc.collect()
            while len(file_list_asc) > 0:
                json_file = json_dir + file_list_asc.pop()
                with open(json_file, 'r') as tmp_f:
                    for line in tmp_f.readlines():
                        line = line.replace('[', '').replace(']', '').replace(",\n", '').replace("\\u0000", "0.0")
                        if line.startswith(','):
                            line = line[1:]
                        if line == '' or line == '\n' or line == "]" or line == '\n]':
                            continue
                        json_dic = json.loads(line)
                        del json_dic['pin.location']
                        writer.writerow(json_dic)
                        del line, json_dic
                    tmp_f.flush()
                    tmp_f.close()
                    print("one file done!")
                del tmp_f, json_file
                csv_f.flush()
                gc.collect()
