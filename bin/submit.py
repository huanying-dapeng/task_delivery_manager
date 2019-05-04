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

from conf import settings

settings.BIN_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.dirname(settings.BIN_DIR))

