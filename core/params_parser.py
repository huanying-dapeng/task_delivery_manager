#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/4 11:56
@file    : params_parser.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import argparse
import os
import sys
import datetime


class ParamsParser(object):
    def __init__(self):
        self.__out_dir = None
        self.__conf_file = None
        self.__parser = None
        self.__is_ready = False
        self._parser()

    def _parser(self):
        if self.__parser is None:
            self.__parser = argparse.ArgumentParser()
        self.__parser.add_argument('-o', '--out_dir', default='./{datetime}', help='output dir [default: %(default)s]')
        self.__parser.add_argument('-c', '--conf_file', required=True,
                                   help='commands info file refering to the configuration file in the test dir')
        print(sys.argv)
        if len(sys.argv) < 3:
            self.__parser.parse_args(['-h'])

        args_obj = self.__parser.parse_args()
        self._args_check(args_obj)

    def _args_check(self, args_obj):
        out_dir = args_obj.out_dir
        if out_dir == './{datetime}':
            date = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            self.__out_dir = os.path.join(os.path.abspath('.'), date)
        flag = 1
        while True:
            if os.path.isdir(self.__out_dir):
                self.__out_dir += '_%s' % flag
                flag += 1
            else:
                os.mkdir(self.__out_dir)
                break

        conf_file = args_obj.conf_file
        if not os.path.isfile(conf_file):
            raise FileNotFoundError('conf_file [%s] is not existent' % conf_file)
        self.__conf_file = conf_file
        self.__is_ready = True

    @property
    def is_ready(self):
        return self.__is_ready

    @property
    def out_dir(self):
        return self.__out_dir

    @property
    def cmd_conf(self):
        return self.__conf_file


if __name__ == '__main__':
    params_obj = ParamsParser()
    if params_obj.is_ready:
        print('==========')
        print(params_obj.__dict__)
