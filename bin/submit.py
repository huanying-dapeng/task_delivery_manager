#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/4 20:42
@file    : submit.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import os
import sys

bin_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(bin_dir))

from conf import settings
settings.BIN_DIR = os.path.abspath(os.path.dirname(__file__))

from core.params_parser import ParamsParser
from core.master import Master


if __name__ == '__main__':
    params_obj = ParamsParser()
    if params_obj.is_ready:
        maser = Master(params_obj)
        maser.run()
