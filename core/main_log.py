#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/4 22:17
@file    : main_log.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import datetime
import logging
import os

from .common_tools import singleton


@singleton
class TaskLogger(object):
    def __init__(self, outdir, logger_name, stream_on=True):
        self.__outdir = outdir

        self.__level = logging.DEBUG
        self.__streem_on = stream_on

        self.__logger = logging.getLogger(logger_name)
        self.__logger.setLevel(self.__level)
        self.__logger.propagate = 0
        self._init_handler()

    def _init_handler(self):
        format = '%(asctime)s    %(name)s    %(levelname)s : %(message)s'
        formatter = logging.Formatter(format, "%Y-%m-%d %H:%M:%S")

        log_path = os.path.join(self.__outdir, datetime.datetime.now().strftime('%Y%m%d') + "_log.txt")
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(self.__level)
        file_handler.setFormatter(formatter)
        self.__logger.addHandler(file_handler)
        self.file_handler = file_handler

        if self.__streem_on:
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(self.__level)
            stream_handler.setFormatter(formatter)
            self.__logger.addHandler(stream_handler)
            self.stream_handler = stream_handler

    def get_logger(self, name=""):
        """
        :param name: logger name
        """
        return self.__logger.getChild(name)

    def destroy(self):
        if self.stream_handler and self.logger:
            self.logger.removeHandler(self.stream_handler)
        if self.file_handler and self.logger:
            self.logger.removeHandler(self.file_handler)
