#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/1/2 23:07
@file    : client.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""

import zerorpc
import msgpack


class Clint(object):
    def __init__(self, endpoint, job_id):
        self.__endpoint = endpoint
        self.__job_id = job_id
        self._clent = None
        # self._clent = zerorpc.Client(connect_to=self.__endpoint)
        self.__statuses = {'running', 'start', 'end'}

    def send(self, msg, status='running'):
        if not isinstance(msg, (str, dict, list, tuple)):
            raise TypeError('msg must str, list, dict, or tupe type')
        if status not in self.__statuses:
            raise ValueError('status must be running, start or end.')
        msg = {'id': self.__job_id, 'info': msg, 'status': status}
        return self._clent.add_msg(msg)

    def __enter__(self):
        self._clent = zerorpc.Client(connect_to=self.__endpoint)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is not None:
            raise Exception(str(exc_tb) + ' ' + str(exc_val))
        self._clent.close()


if __name__ == '__main__':
    c = Clint('tcp://127.0.0.1:8888', '1')
    with c:
        print(c.send({1:2}))
