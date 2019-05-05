#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/3 21:14
@file    : settings.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
from core.common_tools import *

SERVER_HOST = get_host()
SERVER_PORT = get_port(8888)

BIN_DIR = None

CLUSTER_NAME = 'NOHUP'  # PBS or SLURM or NOHUP
PBS_QUEUE_NAME = 'zh'
PBS_TASK_DEMO = '''
#PBS -N {name}
#PBS -l nodes=1:ppn={cpu}
#PBS -l mem={mem}
#PBS -e {stderr}
#PBS -o {stdout}
#PBS -d {work_dir}
#PBS -q {queue_name}
cd {work_dir}

{cmd}
'''
