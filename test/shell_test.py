#!/usr/bin/env python3
# -*- coding: utf-8 -*- 

import time, os

for i in range(20):
    print(os.getpid(), '====')
    time.sleep(1)
