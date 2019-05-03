#!/us :in/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/1/2 23:06
@file    : server.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
from threading import Lock

import zerorpc

from conf.settings import SERVER_PORT


class Server(object):
    __LOCK__ = Lock()

    def __init__(self, bind_obj):
        self.__port = SERVER_PORT
        self.__bind_obj = bind_obj
        self.__server = None

    def __enter__(self):
        if self.__server is None:
            with self.__LOCK__:
                if self.__server is None:
                    self.__server = zerorpc.Server(self.__bind_obj)
                    self.__server.bind('tcp://0.0.0.0:%s' % self.__port)
        self.__server.run()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__server.close()

    def close(self):
        self.__server.close()


if __name__ == '__main__':
    class T:
        def add_msg(self, data):
            print(data)
            return True
    server = Server(T(), 8888)
    with server:
        pass
