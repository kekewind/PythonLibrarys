"""
Convert image or video file into ascii char view
need to config redis and support by ffmpeg to process audio
default support resampling and multi-thread to accelerate the process
"""
import argparse
import gc
import os
import sys
import threading
import time
import uuid
from queue import Queue
from subprocess import Popen, PIPE

import cv2
import numpy
import redis
from PIL import Image, ImageDraw, ImageFont


class VideoToASCII:

    def __init__(self, task_info):
        self._threads = {}
        self.task_info = task_info
        self.ascii_code = list("$@B%8&WM#*oahkbdpqwmZO0QLCJUYXzcvunxrjft/\|()1{}[]?-_+~<>i!lI;:,\"^`'. ")
        self._frame_count = 0
        self._codec = ''
        self._fps = 0
        self._height = 0
        self._width = 0
        self.input_movie = None
        self._video_writer = None
        self.rdb = redis.Redis(host="localhost", port=6379, db=1)
        self.rdb.flushdb()

    def get_video_info(self):
        self.input_movie = cv2.VideoCapture(self.task_info["file_path"])
        # 获取帧数（不准确）
        self._frame_count = int(self.input_movie.get(cv2.CAP_PROP_FRAME_COUNT))
        # 获取编码
        ex = int(self.input_movie.get(cv2.CAP_PROP_FOURCC))
        self._codec = chr(ex & 0XFF) + chr((ex & 0XFF00) >> 8) + chr((ex & 0XFF0000) >> 16) + chr(
            (ex & 0XFF000000) >> 24)
        # 获取帧率
        self._fps = self.input_movie.get(cv2.CAP_PROP_FPS)
        # 视频长宽
        self._height = int(self.input_movie.get(cv2.CAP_PROP_FRAME_HEIGHT))
        self._width = int(self.input_movie.get(cv2.CAP_PROP_FRAME_WIDTH))
        # 建立视频写入对象
        self._video_writer = cv2.VideoWriter('output.avi', cv2.VideoWriter_fourcc(*'DIVX'), self._fps,
                                             (self._width, self._height))

    def video_convert(self):
        self.get_video_info()
        frame_queue = Queue(100)
        frame_queue.join()
        # 建立并启动帧处理线程
        for each in range(self.task_info["thread_count"]):
            job_guid = str(uuid.uuid4())
            self._threads[job_guid] = self.Consumer(frame_queue, job_guid, self)
            self._threads[job_guid].start()
        # 建立并启动队列生产线程
        job_guid = str(uuid.uuid4())
        self._threads[job_guid] = self.Producer(frame_queue, job_guid, self)
        self._threads[job_guid].start()
        self._threads[job_guid].join()
        # 完成视频写入
        self.video_write()
        # 视频音频合成
        self.video_add_audio()

    def image_convert(self):
        img = Image.open(self.task_info["file_path"])
        self._width = img.size[0]
        self._height = img.size[1]
        self._frame_count = 1
        frame = numpy.asarray(img)
        self.frame_to_image({"index": 0, "frame": frame})
        lines = str(self.rdb.get("frame_" + str(0)), encoding="utf-8")
        colors = list(eval(self.rdb.get("color_" + str(0))))
        font = ImageFont.load_default().font
        self.char_to_image(font, lines, colors).save(self.task_info["output"])

    def frame_to_image(self, item):
        frame = item["frame"]
        lines = ""
        colors = []
        # 根据采样间隔逐帧提取像素颜色及字符映射，保存单帧字符串至redis并保留帧序
        for i in range(0, int(self._height), self.task_info["resample"]):
            for j in range(0, int(self._width), self.task_info["resample"]):
                pixel = frame[i, j]
                colors.append((pixel[0], pixel[1], pixel[2]))
                if len(pixel) == 4:
                    lines += self.get_char(pixel[0], pixel[1], pixel[2], pixel[3])
                else:
                    lines += self.get_char(pixel[0], pixel[1], pixel[2])
                del pixel
            lines += '\n'
            colors.append((255, 255, 255))
        self.rdb.set("frame_" + str(item["index"]), lines)
        self.rdb.set("color_" + str(item["index"]), colors)
        del lines, colors, frame
        gc.collect()
        res = {
            "percent": float(len(self.rdb.keys("frame_*")) * 100 / self._frame_count),
            "num": int(len(self.rdb.keys("frame_*")))
        }
        sys.stdout.write(
            "\r" + "已完成:%.3f%%，正在完成：第%d帧，耗时：%s秒" % (
                res["percent"], res["num"], round(time.process_time(), 2)))

    def get_char(self, r, g, b, alpha=256):
        if alpha == 0:
            return ''
        length = len(self.ascii_code)
        # 灰度字符映射
        gray = int(0.2126 * r + 0.7152 * g + 0.0722 * b)
        unit = (256.0 + 1) / length
        return self.ascii_code[int(gray / unit)]

    def video_write(self):
        print("Start write video")
        font = ImageFont.load_default().font
        for i in range(self._frame_count):
            if self.rdb.exists("frame_" + str(i)):
                # 建立字符图片
                lines = str(self.rdb.get("frame_" + str(i)), encoding="utf-8")
                colors = list(eval(self.rdb.get("color_" + str(i))))
                # 图片转帧
                frame = numpy.asarray(self.char_to_image(font, lines, colors))
                self._video_writer.write(frame)
                del frame, colors, lines
                sys.stdout.write("\r" + str(i) + " frames already been write")
        self._video_writer.release()

    def char_to_image(self, font, lines, colors):
        tx_im = Image.new("RGB", (self._width, self._height), (255, 255, 255))
        dr = ImageDraw.Draw(tx_im)
        x = y = 0
        # 获取字体的宽高
        font_w, font_h = font.getsize(lines[1])
        # 优化显示效果
        font_h *= 0.8
        font_w *= 1.3  # 调整后更佳
        # ImageDraw为每个ascii码进行上色
        for j in range(len(lines)):
            if lines[j] == '\n':
                x += font_h
                y = -font_w
            # 逐行逐字写入
            dr.text((y, x), lines[j], font=font, fill=colors[j])
            y += font_w
        return tx_im

    def video_add_audio(self):
        outfile_name = str(uuid.uuid4()) + '.mp3'
        if os.path.isfile(self.task_info["output"]):
            os.remove(self.task_info["output"])
        # 提取音频
        continues_res_acc = Popen('ffmpeg -i "' + self.task_info["file_path"] + '" -f mp3 ' + outfile_name, shell=True,
                                  stdout=PIPE)
        for i in iter(continues_res_acc.stdout.readline, 'b'):
            if str(i, encoding='gbk') == "":
                break
        # 合成音视频
        continues_res_vic = Popen(
            'ffmpeg -i output.avi -i ' + outfile_name + ' -strict -2 -f mp4 "' + self.task_info["output"]+'"',
            shell=True, stdout=PIPE)
        for i in iter(continues_res_vic.stdout.readline, 'b'):
            if str(i, encoding='gbk') == "":
                break
        continues_res_acc.terminate()
        continues_res_vic.terminate()
        os.remove(outfile_name)
        os.remove("output.avi")

    class Consumer(threading.Thread):
        def __init__(self, queue, name, parent):
            threading.Thread.__init__(self)
            self.queue = queue
            self.name = name
            self.out = parent
            self.daemon = True

        def run(self):
            while True:
                item = self.queue.get()
                self.out.frame_to_image(item)
                self.queue.task_done()

    class Producer(threading.Thread):
        """
        @:param queue 阻塞队列
        @:param name 线程名字
        """

        def __init__(self, queue, name, parent):
            threading.Thread.__init__(self)
            self.queue = queue
            self.name = name
            self.out = parent
            self.daemon = True

        def run(self):
            ret, frame = self.out.input_movie.read()
            count = 0
            while True:
                while ret:
                    if isinstance(frame, numpy.ndarray):
                        self.queue.put({"index": count, "frame": frame})
                        count = count + 1
                        del frame
                    ret, frame = self.out.input_movie.read()
                    if not ret:
                        self.out.input_movie.release()
                if self.queue.empty():
                    break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', type=str, help="path of image or video file")  # 文件路径
    parser.add_argument('-p', '--type', type=str, default="video", choices=["image", "video"],
                        help="image or video")  # 文件类型
    parser.add_argument('-r', '--resample', type=int, default=8, choices=[4, 8, 16],
                        help="frame resampling interval")  # 采样间隔
    parser.add_argument('-o', '--output', type=str)  # 输出文件路径
    parser.add_argument('-t', '--thread_num', type=int, default=16, choices=[4, 8, 16])  # 线程数
    args = parser.parse_args()
    task = {
        "file_path": args.file,
        "file_type": args.type,
        "thread_count": args.thread_num,
        "output": args.output,
        "resample": args.resample
    }
    if not task["file_path"] or not task["output"]:
        tmp = input("请输入要处理的图像或视频文件：")
        out = input("请输入输出文件路径：")
        f_type = input("请输入文件类型：")
        if os.path.isfile(tmp):
            task["file_path"] = tmp
        if not os.path.isdir(out):
            task["output"] = out
        task["file_type"] = f_type
    video_parser = VideoToASCII(task)
    if task["file_type"] == "video":
        video_parser.video_convert()
    elif task["file_type"] == "image":
        video_parser.image_convert()
