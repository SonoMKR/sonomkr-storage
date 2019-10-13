#!/usr/bin/python3

import zmq
from datetime import datetime, timedelta
import time

zmq_context = zmq.Context()
zmq_channel_1 = zmq_context.socket(zmq.PUB)
zmq_channel_1.bind("tcp://*:6661")
zmq_channel_2 = zmq_context.socket(zmq.PUB)
zmq_channel_2.bind("tcp://*:6662")

nowtime = datetime.now()

while True:
    now = nowtime.strftime("%Y-%m-%d %H:%M:%S.000")
    # print(now)
    data = now.encode("utf-8") + b";12:30.0;13:30.0;14:30.0;15:30.0;16:30.0;17:30.0;18:30.0;19:30.0;20:30.0;21:30.0;22:30.0;23:30.0;24:30.0;25:30.0;"
    msg = [b"LEQ", data]
    zmq_channel_1.send_multipart(msg)
    zmq_channel_2.send_multipart(msg)
    time.sleep(0.99)
    nowtime = nowtime + timedelta(seconds=1)
