#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/4 12:29
@file    : worker.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import sys
import time
from subprocess import Popen
from threading import Thread

from core.client import Client

class Worker(Thread):
    """A class that represents a task of job scheduling system [PBS or SLURM].

    """
    def __init__(self, args_obj):
        super(Worker, self).__init__()
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
    def __init__(self, cmd_obj: Worker):
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