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
from gevent import Greenlet, sleep

from conf.settings import SERVER_PORT, SERVER_HOST


class Server(Greenlet):
    __LOCK__ = Lock()

    def __init__(self, bind_obj):
        super(Server, self).__init__()
        self.__port = SERVER_PORT
        self.__bind_obj = bind_obj
        self.__server = None

    def __conn(self):
        if self.__server is None:
            with self.__LOCK__:
                if self.__server is None:
                    self.__server = zerorpc.Server(self.__bind_obj)
                    self.__server.bind('tcp://0.0.0.0:%s' % self.__port)

    def __enter__(self):
        if self.__server is None:
            self.__conn()
        # self.__server.run()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.__server is not None:
            self.__server.close()
            self.__server = None

    def _run(self):
        super()._run()
        if self.__server is None:
            self.__conn()
        self.__server.run()

    @property
    def endpoint(self):
        return 'tcp://%s:%s' % (SERVER_HOST, SERVER_PORT)


class ServerWorker(object):
    def __init__(self, master):
        self.master = master

    def add_msg(self, data):
        worker_id = data['id']
        status = data['status']
        info = data['info']
        self.master.logger.debug('worker_id: %s, status: %s, info: %s' % (worker_id, status, str(info)))
        if status in ('end', 'error'):
            self.master[worker_id].status = status
            self.master[worker_id].is_end = True
        elif status == 'running':
            self.master[worker_id].is_running = True

        if all([bool(agent) for agent in self.master.values()]):
            self.master.

if __name__ == '__main__':
    class T:
        def add_msg(self, data):
            print(data)
            return True
    server = Server(T())

    with server:
        print(server.endpoint)
        server.start()
    server.join()
