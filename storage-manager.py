#!/usr/bin/python3

from typing import Any, List
import argparse
from datetime import timedelta
from datetime import datetime
import libconf
import os
import re
import sys
import asyncio
from asyncio import Task
from concurrent.futures import CancelledError
import zmq
import zmq.asyncio
import sqlite3

index_to_freq: List[str] = ["0.8Hz", "1Hz", "1.25Hz", "1.6Hz", "2Hz", "2.5Hz", "3.15Hz", "4Hz", "5Hz", "6.3Hz", "8Hz", "10Hz", "12.5Hz", "16Hz", "20Hz", "25Hz", "31.5Hz", "40Hz", "50Hz", "63Hz", "80Hz", "100Hz", "125Hz", "160Hz", "200Hz", "250Hz", "315Hz", "400Hz", "500Hz", "630Hz", "800Hz", "1kHz", "1.25kHz", "1.6kHz", "2kHz", "2.5kHz", "3.15kHz", "4kHz", "5kHz", "6.3kHz", "8kHz", "10Hz", "12.5kHz", "16kHz", "20kHz"]

parser = argparse.ArgumentParser(description="Data storage manager for the SonoMKR Project")
parser.add_argument("-c", "--conf", dest="conf", default="./storage.conf", help="The location of the storage configuration file")
parser.add_argument("-d", "--dir", dest="dir", default="./data", help="The directory where data will be stored.")
parser.add_argument("--display-conf", dest="display_conf", action="store_true", help="Set this flag to display current config and return")

args = parser.parse_args()

class StorageChannel:

    stop: bool = False

    duration: int = 60

    dir: str = "./"
    base_name: str = ""
    format: str = "csv"
    extension: str = ".csv"

    zmq_context: Any
    zmq_socket: Any
    zmq_address: str
    zmq_topic: str

    new_file: bool = True
    current_file: str = None
    last_time: datetime = None

    def __init__(self, zmq_context, config):

        self.zmq_context = zmq_context

        if not config.zmqAddress:
            raise Exception("Channel config misses the 'zmqAddress' parameter")
        self.zmq_address = config.zmqAddress

        if not config.zmqTopic:
            raise Exception("Channel config misses the 'zmqTopic' parameter")
        self.zmq_topic = config.zmqTopic

        self.zmq_socket = self.zmq_context.socket(zmq.SUB)
        self.zmq_socket.connect(self.zmq_address)
        self.zmq_socket.subscribe(self.zmq_topic)

        if config.duration:
            self.duration = config.duration
        if config.dataDir:
            self.dir = config.dataDir
        if config.format:
            self.format = config.format
        if self.format == "sqlite":
            self.extension = ".db"
        if config.fileBaseName:
            self.base_name = config.fileBaseName

        if self.base_name is not "" and self.base_name[-1] is not "_":
            self.base_name += "_"

        os.makedirs(self.dir, exist_ok=True)
        if not os.access(self.dir, os.X_OK | os.W_OK):
            raise PermissionError("You don't have persmssion to write files to this folder")

    def process_msg(self, msg):
        time = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3});", msg.decode("utf-8")).group(1)
        start_time = datetime.fromisoformat(time)

        data_matches = re.finditer(r"(\d{1,2}):(\d{1,2}.\d);", msg.decode("utf-8"))

        freqs: List[int] = []
        values: List[float] = []
        for match in data_matches:
            freqs.append(int(match.group(1)))
            values.append(float(match.group(2)))

        end_of_file = ((((start_time.hour * 60 + start_time.minute) % self.duration) == 0) and (start_time.second == 0) and (start_time.microsecond == 0))

        if (self.new_file or end_of_file or (start_time - self.last_time).total_seconds() > 1):

            if (self.current_file is not None):
                new_time = self.last_time + timedelta(seconds=1)
                os.rename(self.current_file, self.current_file + new_time.strftime("_%H%M%S") + self.extension)
    
            self.current_file = self.dir + "/" + self.base_name + start_time.strftime("%Y%m%d_%H%M%S")

            if (self.format == 'csv'):
                with open(self.current_file, 'a+') as f:
                    header = "datetime;"
                    for freq in freqs:
                        header += f"{index_to_freq[freq]};"
                    f.write(header + "\n")

            if (self.format == 'sqlite'):
                conn = sqlite3.connect(self.current_file)
                query = "CREATE TABLE data (datetime INTEGER, "
                query += ", ".join(map(lambda freq: f"'{index_to_freq[freq]}' REAL", freqs))
                query += ")"
                conn.execute(query)
                conn.commit()
                conn.close()

            self.new_file = False

        if (self.format == 'csv'):
            with open(self.current_file, 'a+') as f:
                data = start_time.strftime("%Y%m%d_%H%M%S") + ";"
                for val in values:
                    data += f"{val};"
                f.write(data + "\n")

        if (self.format == 'sqlite'):
            conn = sqlite3.connect(self.current_file)
            query = "INSERT INTO data VALUES("
            query += f"'{int(start_time.timestamp()*1000)}', "
            query += ", ".join(map(lambda val: f"'{val}'", values))
            query += ")"
            conn.execute(query)
            conn.commit()
            conn.close()

        self.last_time = start_time


    @asyncio.coroutine
    def recv_and_process(self):
        msg = yield from self.zmq_socket.recv_multipart() # waits for msg to be ready
        self.process_msg(msg[1])
        asyncio.ensure_future(self.recv_and_process())

    def close_current_file(self):
        if self.last_time is not None:    
            new_time = self.last_time + timedelta(seconds=1)
            os.rename(self.current_file, self.current_file + new_time.strftime("_%H%M%S") + self.extension)


class StorageManager:

    channels: List[StorageChannel] = []

    def __init__(self, args):

        self.args = args

        try:
            with open(args.conf, 'r') as conf:
                self.conf = libconf.load(conf)
        except OSError as err:
            sys.stderr.write(f"[ERROR] storage config file {args.conf} open error : {err}\n")
            exit(1)

        if (args.display_conf):
            self.display_conf_and_exit()

        self.zmq_context = zmq.asyncio.Context()

        for channel_cfg in self.conf.channels:
            try:
                channel = StorageChannel(self.zmq_context, channel_cfg)
            except Exception as err:
                sys.stderr.write(f"[ERROR] channel config error :\n{channel_cfg}\n Error message : {err}\n")
            self.channels.append(channel)

        self.start_listening()

        
    def start_listening(self):
        loop = asyncio.get_event_loop()
        try:
            for channel in self.channels:
                asyncio.ensure_future(channel.recv_and_process())
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            for channel in self.channels:
                channel.close_current_file()
            loop.close()

    def display_conf_and_exit(self):
        print(libconf.dumps(self.conf))
        exit(0)


StorageManager(args)

