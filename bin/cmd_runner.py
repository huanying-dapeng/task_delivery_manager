#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/4 20:47
@file    : cmd_runner.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import os
import time
from threading import Thread
import argparse
import sys
from subprocess import Popen

# add project path to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.client import Client

class Command(Thread):
    def __init__(self, args_obj):
        super(Command, self).__init__()
        self.cmd = args_obj.cmd
        self.endpoint = args_obj.endpoint
        self.worker_id = args_obj.worker_id
        self.status = 'wait'  # running, end, error
        self.name = self.worker_id
        self.start_time = time.time()

    def run_cmd(self):
        process = Popen(self.cmd, shell=True, universal_newlines=True, stdout=sys.stdout, stderr=sys.stderr)
        self.status = 'running'
        process.wait()
        sys.stdout.flush()
        sys.stderr.flush()
        returncode = process.returncode
        if returncode == 0:
            self.status = 'end'
        else:
            self.status = 'error'

    def run(self):
        self.run_cmd()


class MSGManager(Thread):
    def __init__(self, cmd_obj: Command):
        super(MSGManager, self).__init__()
        self.__cmd_obj = cmd_obj
        self.__client = Client(cmd_obj.endpoint, cmd_obj.worker_id)

    def run(self):
        while True:
            with self.__client as client:
                msg = {'used_time': round(time.time() - self.__cmd_obj.start_time, 5)}
                client.send(msg, status=self.__cmd_obj.status)
            if self.__cmd_obj.status in ('end', 'error'):
                break
            time.sleep(15)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--worker_id', help='worker name which is unique in the whole work')
    parser.add_argument('-e', '--endpoint', help="server's endpoint [protocol + ip + port]")
    parser.add_argument('-c', '--cmd', help='command line string')

    if len(sys.argv) < 7:
        parser.parse_args(['-h'])

    args_obj = parser.parse_args()
    print(os.path.dirname(os.path.dirname(__file__)))
    cmd_obj = Command(args_obj=args_obj)
    msg_obj = MSGManager(cmd_obj)
    msg_obj.start()
    cmd_obj.start()
    cmd_obj.join()
    msg_obj.join()
