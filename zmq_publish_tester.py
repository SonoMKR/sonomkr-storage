#!/usr/bin/python3

import zmq
from datetime import datetime
import time

zmq_context = zmq.Context()
zmq_channel_1 = zmq_context.socket(zmq.PUB)
zmq_channel_1.bind("tcp://*:6661")
zmq_channel_2 = zmq_context.socket(zmq.PUB)
zmq_channel_2.bind("tcp://*:6662")

while True:
    # now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.000")
    data = now.encode("utf-8") + b";12:30.0;13:30.0;14:30.0;15:30.0;16:30.0;17:30.0;18:30.0;19:30.0;20:30.0;21:30.0;22:30.0;23:30.0;24:30.0;25:30.0;"
    msg = [b"LEQ", data]
    zmq_channel_1.send_multipart(msg)
    zmq_channel_2.send_multipart(msg)
    print(data)
    time.sleep(1)