#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/4 0:15
@file    : common_tools.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import socket

__all__ = ['get_port', 'get_host']


def get_host():
    host = None
    try:
        hostname = socket.gethostname()
        host = socket.gethostbyname(hostname)
    except:
        pass
    finally:
        if host is None or host == '127.0.0.1':
            sk = socket.socket(type=socket.SOCK_DGRAM)
            sk.connect(('8.8.8.8', 80))
            host = sk.getsockname()[0]
    return host


def get_port(port):
    try:
        sk = socket.socket()
        sk.connect(('127.0.0.1', port))
        sk.close()
        return get_port(port + 1)
    except ConnectionRefusedError:
        return port
