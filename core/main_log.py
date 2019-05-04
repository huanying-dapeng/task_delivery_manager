#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/4 22:17
@file    : main_log.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import logging
import os

from gevent import lock, Greenlet


class TaskLogger(Greenlet):
    __loggers_obj = dict()
    __LOCK__ = lock.BoundedSemaphore(1)

    def __init__(self, outdir, logger_name, stream_on=True):
        super(TaskLogger, self).__init__()
        self.__outdir = outdir

        self.__level = logging.DEBUG
        self.__streem_on = stream_on
        self.__format = '%(asctime)s    %(name)s    %(levelname)s : %(message)s'
        self.__formatter = logging.Formatter(self.__format, "%Y-%m-%d %H:%M:%S")

        self.__log_path = os.path.join(self.__outdir, "log.txt")
        self.__file_handler = logging.FileHandler(self.__log_path)
        self.__file_handler.setLevel(self.__level)
        self.__file_handler.setFormatter(self.__formatter)

        if self.__streem_on:
            self.__stream_handler = logging.StreamHandler()
            self.__stream_handler.setLevel(self.__level)
            self.__stream_handler.setFormatter(self.__formatter)

        self.__logger = None

    def get_logger(self, name=""):
        """
        :param name: logger name
        """
        self.__logger = logging.getLogger(name)
        self.__logger.propagate = 0
        self._add_handler(self.__logger)
        return self.__logger

    def _add_handler(self, logger):
        logger.setLevel(self.__level)
        logger.addHandler(self.__file_handler)
        if self.__streem_on:
            logger.addHandler(self.__stream_handler)

    def __new__(cls, *args, **kwargs):
        logger_name = args[1]
        # Double-Checked Locking: to increase concurrency
        if not cls.__loggers_obj.get(logger_name):
            with cls.__LOCK__:
                if not cls.__loggers_obj.get(logger_name):
                    cls.__loggers_obj[logger_name] = super().__new__(cls)
        return cls.__loggers_obj[logger_name]

