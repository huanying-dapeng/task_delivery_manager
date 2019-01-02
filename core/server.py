#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/1/2 23:06
@file    : server.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
from multiprocessing.managers import BaseManager
from multiprocessing import Process, Manager, Array
import threading
from threading import RLock, BoundedSemaphore
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from ctypes import c_int, c_char, c_double, Structure

WINDOWS = os.name == "nt"
Lock = RLock if WINDOWS else BoundedSemaphore(1)


class Massage(Structure):
    _fields_ = [
        ('name', c_char * 1024),
        ('cpu', c_int),
        ('mem', c_double),
        ('cmd', c_char * 1024)
    ]


    # def __str__(self):
    #     return self.name


class TasksManager(Process):
    def __init__(self):
        super(TasksManager, self).__init__()
        self.receive = Array(Massage, 200)
        a = Massage(b"a",2,2.3,b"aldkf;alsdkflaksdfla")
        # self.receive[0] = Massage("a",2,2.3,"aldkf;alsdkflaksdfla")
        print(a.name)

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