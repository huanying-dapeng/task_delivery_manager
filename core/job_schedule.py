#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/5 16:02
@file    : job_schedule.py
@author  : zhipeng.zhao
@contact: 757049042@qq.com
"""
import os
import re

from gevent import sleep

from conf.settings import (
    PBS_TASK_DEMO, BIN_DIR, PBS_QUEUE_NAME
)
from core.main_log import TaskLogger


class PBS(object):
    """
        openPBS,
    """

    def __init__(self, agent):
        self.agent = agent
        self.logger = TaskLogger(self.agent.master.work_dir, 'MAIN-SERVER').get_logger('PBS')
        self.work_dir = self.agent.master.work_dir
        self.__submit_times = 0
        self.__pbs_file = None
        self.id = None

    def create_file(self):
        """
        create PBS task file

        :return:
        """
        file_path = os.path.join(self.work_dir, self.agent.name + ".pbs")
        script = os.path.join(BIN_DIR, "cmd_runner.py")
        cpu, mem = self.agent.get_resource()
        with open(file_path, "w") as f:
            cmd = "python %s -i %s -e %s -c %s\n" % (
            script, self.agent.name, self.agent.master.endpoint, self.agent.cmd)
            content = PBS_TASK_DEMO.format(
                name=self.agent.name,
                cpu=cpu,
                mem=mem,
                stderr=self.agent.name + '.e',
                stdout=self.agent.name + '.o',
                work_dir=self.agent.master.work_dir,
                cmd=cmd,
                queue_name=PBS_QUEUE_NAME
            )
            f.write(content)

        return file_path

    def submit(self):
        """
        submit PBS task

        :return: jobid
        """
        if self.__pbs_file is None:
            self.__pbs_file = self.create_file()
        if self.__submit_times == 11:
            self.logger.error('PBS: %s -- delivery failed')
            return None

        output = os.popen('qsub %s' % self.__pbs_file)
        self.__submit_times += 1
        text = output.read()
        if re.match(r'Maximum number', text):
            self.logger.warn("Reach maximum number, retry in 30 second!")
            sleep(30)
            self.submit()
        else:
            m = re.search(r'(\d+)\..*', text)
            if m:
                self.id = m.group(1)
                self.logger.debug('%s PBS job id: %s' % (self.agent.worker_id, self.id))
                return self.id
            else:
                self.logger.warn("PBS error:%s, retry in 30 second!\n" % output)
                sleep(30)
                self.submit()

    def delete(self):
        """
        del current task

        :return: None
        """

        if self.check_state():
            os.system('qdel -p %s' % self.id)

    def check_state(self):
        """
        check current task status

        :return: string status code [Q, R, ...] or False
        """
        output = os.popen('qstat -f %s' % self.id)
        text = output.read()
        m = re.search(r"job_state = (\w+)", text)
        if m:
            return m.group(1)
        else:
            return False


class NOHUP(object):
    """
        NOHUP task
    """

    def __init__(self, agent):
        self.agent = agent
        self.logger = TaskLogger(self.agent.master.work_dir, 'MAIN-SERVER').get_logger('PBS')
        self.work_dir = self.agent.master.work_dir
        self.__submit_times = 0
        self.__nohup_cmd = None
        self.id = None

    def create_cmd(self):
        """
        create PBS task file

        :return:
        """
        file_path = os.path.join(self.work_dir, self.agent.name + ".sh")
        script = os.path.join(BIN_DIR, "cmd_runner.py")
        with open(file_path, "w") as f:
            cmd = 'python %s -i %s -e %s -c \"%s\"' % (
            script, self.agent.name, self.agent.master.endpoint, self.agent.cmd)

            content = 'nohup {cmd} > {stdout} 2> {stderr} & echo $!'.format(
                cmd=cmd,
                stdout=os.path.join(self.work_dir, self.agent.name + '.o'),
                stderr=os.path.join(self.work_dir, self.agent.name + '.e'),
            )
            f.write('#!/bin/bash\n' + content)

        return content

    def submit(self):
        """
        submit PBS task

        :return: jobid
        """
        if self.__nohup_cmd is None:
            self.__nohup_cmd = self.create_cmd()

        output = os.popen(self.__nohup_cmd)
        self.__submit_times += 1
        text = output.read()
        self.agent.master.logger.debug('NOHUP Process ID:' + text.strip())
        self.id = int(text.strip())
        return self.id

    def delete(self):
        """
        del current task

        :return: None
        """
        os.system('kill -9 %s' % self.id)
