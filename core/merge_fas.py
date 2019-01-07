#!/us :in/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/1/2 23:06
@file    : server.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""


def merge(res_list):
    for fhandler in res_list:
        for line in fhandler:
            line = ''
            if line.startswith('>') or line.startswith('@'):
                print(line)


if __file__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', help='merge files list')

    merge([])
