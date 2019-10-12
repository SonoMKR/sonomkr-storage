#!/usr/bin/python3

import argparse
from datetime import timedelta
from datetime import datetime
import libconf
import os
import re
import sys
import asyncio
import zmq
import zmq.asyncio
import sqlite3

parser = argparse.ArgumentParser(description="Data storage manager for the SonoMKR Project")
parser.add_argument("-c", "--conf", dest="conf", default="./storage.conf", help="The location of the storage configuration file")
parser.add_argument("-d", "--dir", dest="dir", default="./data", help="The directory where data will be stored")
parser.add_argument("--display-conf", dest="display_conf", action="store_true", help="Set this flag to display current config and return")

args = parser.parse_args()

class StorageManager:

    def __init__(self, args):

        self.args = args

        try:
            with open(args.conf, 'r') as conf:
                self.conf = libconf.load(conf)
        except OSError as err:
            sys.stderr.write("[ERROR] storage config file %s open error : %s \n" % (args.conf, err))
            exit(1)

        if (args.display_conf):
            self.display_conf_and_exit()

        try:
            os.makedirs(args.dir, exist_ok=True)
            if not os.access(args.dir, os.X_OK | os.W_OK):
                raise PermissionError("You don't have persmssion to write files to this folder")
            os.makedirs(args.dir + "/channel_1", exist_ok=True)
            os.makedirs(args.dir + "/channel_2", exist_ok=True)
            self.dirs = [args.dir + "/channel_1", args.dir + "/channel_2"]
        except Exception as err:
            sys.stderr.write("[ERROR] data directory %s error : %s \n" % (args.dir, err))
            exit(1)

        try:
            with open(self.conf.coreConf, 'r') as conf:
                self.core_conf = libconf.load(conf)
        except OSError as err:
            sys.stderr.write("[ERROR] core config file %s open error : %s \n" % (self.conf.coreConf, err))
            exit(1)

        self.zmq_context = zmq.asyncio.Context()
        self.zmq_channel_1 = self.zmq_context.socket(zmq.SUB)
        self.zmq_channel_1.connect("tcp://127.0.0.1:6661")
        self.zmq_channel_1.subscribe("LEQ")
        self.zmq_channel_2 = self.zmq_context.socket(zmq.SUB)
        self.zmq_channel_2.connect("tcp://127.0.0.1:6662")
        self.zmq_channel_2.subscribe("LEQ")

        self.new_file = [True, True]
        self.last_time = [None, None]
        self.current_file = [None, None]
        self.format = [self.conf.channel1.format, self.conf.channel2.format]

        self.start_listening()

    def process_msg(self, msg, channel):
        directory = self.dirs[channel-1]
        duration = int(self.conf.storage.duration)
        time = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3});", msg.decode("utf-8")).group(1)
        start_time = datetime.fromisoformat(time)

        data_matches = re.finditer(r"(\d{1,2}):(\d{1,2}.\d);", msg.decode("utf-8"))

        freqs = []
        values = []
        for match in data_matches:
            freqs.append(match.group(1))
            values.append(match.group(2))

        end_of_file = ((((start_time.hour * 60 + start_time.minute) % duration) == 0) and (start_time.second == 0))

        if (self.new_file[channel-1] or end_of_file or (start_time - self.last_time[channel-1]).total_seconds() > 1):

            print(self.current_file[channel-1])
            if (self.current_file[channel-1] is not None):
                ext = '.csv'
                if (self.format[channel-1] == 'sqlite'):
                    ext = ".db"
                new_time = self.last_time[channel-1] + timedelta(seconds=1)
                os.rename(self.current_file[channel-1], self.current_file[channel-1] + new_time.strftime("_%H%M%S") + ext)
    
            self.current_file[channel-1] = directory + "/" + start_time.strftime("%Y%m%d_%H%M%S")

            if (self.format[channel-1] == 'csv'):
                with open(self.current_file[channel-1], 'a+') as f:
                    header = "datetime"
                    for freq in freqs:
                        header += "%s;" % (freq)
                    f.write(header + "\n")

            if (self.format[channel-1] == 'sqlite'):
                conn = sqlite3.connect(self.current_file[channel-1])
                query = "CREATE TABLE data (datetime INTEGER, "
                query += ", ".join(map(lambda f: "'%s' REAL" % (f), freqs))
                query += ")"
                print(query)
                conn.execute(query)
                conn.commit()
                conn.close()

            print(self.current_file[channel-1])
            self.new_file[channel-1] = False

        if (self.format[channel-1] == 'csv'):
            with open(self.current_file[channel-1], 'a+') as f:
                data = start_time.strftime("%Y%m%d_%H%M%S") + ";"
                for val in values:
                    data += "%s;" % (val)
                f.write(data + "\n")

        if (self.format[channel-1] == 'sqlite'):
            conn = sqlite3.connect(self.current_file[channel-1])
            query = "INSERT INTO data VALUES("
            query += "'%s', " % start_time.timestamp()
            query += ", ".join(map(lambda v: "'%s'" % (v), values))
            # for val in values:
            #     query += "%s, " % (val)
            query += ")"
            conn.execute(query)
            conn.commit()
            conn.close()

        self.last_time[channel-1] = start_time

    @asyncio.coroutine
    def recv_and_process(self, socket, channel):
        msg = yield from socket.recv_multipart() # waits for msg to be ready
        print(msg[1])
        self.process_msg(msg[1], channel)
        asyncio.ensure_future(self.recv_and_process(socket, channel))
        
    def start_listening(self):
        loop = asyncio.get_event_loop()
        try:
            asyncio.ensure_future(self.recv_and_process(self.zmq_channel_1, 1))
            asyncio.ensure_future(self.recv_and_process(self.zmq_channel_2, 2))
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            loop.close()

    def display_conf_and_exit(self):
        print(libconf.dumps(self.conf_file))
        exit(0)



StorageManager(args)

