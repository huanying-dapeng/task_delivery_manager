#!/us :in/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/1/2 23:06
@file    : server.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import os
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from ctypes import Structure, c_char, c_double, c_int
from multiprocessing import Array, Manager, Process
from multiprocessing.managers import BaseManager
from threading import BoundedSemaphore, RLock

WINDOWS = os.name == "nt"
Lock = RLock if WINDOWS else BoundedSemaphore(1)


class Massage(Structure):
    _fields_ = [('name', c_char * 1024), ('cpu', c_int), ('mem', c_double),
                ('cmd', c_char * 1024)]


class TasksManager(Process):
    def __init__(self):
        super(TasksManager, self).__init__()
        self.receive = Array(Massage, 200)
        self.receive[0] = Massage(b"a", 2, 2.3, b"aldkf;alsdkflaksdfla")


class MainSerser(object):
    __obj = None
    __lock__ = Lock()

    def __init__(self):
        self.__task_manager = TasksManager()

    def __new__(cls, *args, **kwargs):
        if cls.__obj is None:
            with cls.__lock__:
                if cls.__obj is None:
                    cls.__obj = super().__new__(cls)
        return cls.__obj


if __name__ == '__main__':
    TasksManager()
