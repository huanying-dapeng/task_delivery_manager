#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/4 20:47
@file    : cmd_runner.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import argparse
import os
import sys

# add project path to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.worker import Worker, MSGManager


def run(args_obj):
    # create worker thread
    cmd_obj = Worker(args_obj=args_obj)
    # create msg manager thread
    msg_obj = MSGManager(cmd_obj)
    msg_obj.start()
    cmd_obj.start()
    cmd_obj.join()
    msg_obj.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--worker_id', help='worker name which is unique in the whole work')
    parser.add_argument('-e', '--endpoint', help="server's endpoint [protocol + ip + port]")
    parser.add_argument('-c', '--cmd', help='command line string')

    if len(sys.argv) < 7:
        parser.parse_args(['-h'])

    run(parser.parse_args())
