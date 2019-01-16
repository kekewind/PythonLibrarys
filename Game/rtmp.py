import cv2
import time
import subprocess as sp

from pkg_resources import resource_filename

ss = 'ffmpeg -re -i mide500_hd18P2P.mp4 -vcodec libx264 -acodec aac -f flv "rtmp://180.76.142.216:1935/live/stream"'
rtmpUrl = 'rtmp://180.76.142.216:1935/live/stream'

# 视频来源 地址需要替换自己的可识别文件地址

camera = cv2.VideoCapture(0)  # 从文件读取视频
# 这里的摄像头可以在树莓派3b上使用
# camera = cv2.VideoCapture(0) # 参数0表示第一个摄像头 摄像头读取视频
# if (camera.isOpened()):# 判断视频是否打开
#     print 'Open camera'
# else:
#     print 'Fail to open camera!'
#     return
# camera.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)  # 2560x1920 2217x2217 2952×1944 1920x1080
# camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
# camera.set(cv2.CAP_PROP_FPS, 5)

# 视频属性
size = (int(camera.get(cv2.CAP_PROP_FRAME_WIDTH)), int(camera.get(cv2.CAP_PROP_FRAME_HEIGHT)))
sizeStr = str(size[0]) + 'x' + str(size[1])
fps = camera.get(cv2.CAP_PROP_FPS)  # 30p/self
fps = int(fps)
hz = int(1000.0 / fps)

# 视频文件保存
fourcc = cv2.VideoWriter_fourcc(*'XVID')
# 管道输出 ffmpeg推送rtmp 重点 ： 通过管道 共享数据的方式
command = [resource_filename(__name__, "ffmpeg.exe"),
           '-y',
           '-f', 'rawvideo',
           '-vcodec', 'rawvideo',
           '-pix_fmt', 'bgr24',
           '-s', sizeStr,
           '-r', str(fps),
           '-i', '-',
           '-c:v', 'libx264',
           '-acodec', 'aac',
           '-pix_fmt', 'yuv420p',
           '-f', 'flv',
           rtmpUrl]
pipe = sp.Popen(command, stdin=sp.PIPE, shell=False)  # ,shell=False

lineWidth = 1 + int((size[1] - 400) / 400)  # 400 1 800 2 1080 3
textSize = size[1] / 1000.0  # 400 0.45
heightDeta = size[1] / 20 + 10  # 400 20
count = 0
faces = []
while True:
    count = count + 1
    ret, frame = camera.read()  # 逐帧采集视频流
    if not ret:
        break
    fpsshow = "Fps  :" + str(int(fps)) + "  Frame:" + str(count)
    nframe = "Play :" + str(int(count / fps))
    ntime = "Time :" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if (count % fps == 0):
        print(fpsshow + " " + ntime)
    pipe.stdin.write(frame.tostring())  # 存入管道

    pass
camera.release()
print("Over!")
