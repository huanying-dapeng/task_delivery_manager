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
from abc import ABC, abstractmethod

from gevent import sleep
from psutil import Process, NoSuchProcess, ZombieProcess, AccessDenied

from conf.settings import (
    PBS_TASK_DEMO, BIN_DIR, PBS_QUEUE_NAME
)
from core.main_log import TaskLogger


class ABSClusterManager(ABC):
    def __init__(self, agent):
        self.agent = agent
        self.work_dir = self.agent.master.work_dir
        self.__submit_times = 0
        self.__cmd_data = None
        self.id = None
        self.logger = None

    @abstractmethod
    def create_cmd(self):
        """
        create PBS/SLURM task file or create shell command
        :return: file path or cmd string
        """
        pass

    @abstractmethod
    def submit(self):
        """
        submit PBS/SLURM/shell task

        :return: jobid
        """
        pass

    @abstractmethod
    def delete(self):
        """
        del current task

        :return: None
        """
        pass

    @abstractmethod
    def check_state(self):
        """
        check current task status

        :return: string status code [Q, R, ...] / True or False
        """
        pass

    @property
    def submit_times(self):
        return self.__submit_times

    @submit_times.setter
    def submit_times(self, value):
        assert isinstance(value, int), 'submit_times value must be int'
        self.__submit_times = value

    @property
    def cmd_data(self):
        return self.__cmd_data

    @cmd_data.setter
    def cmd_data(self, value):
        self.__cmd_data = value


class PBS(ABSClusterManager):
    """
        openPBS,
    """

    def __init__(self, agent):
        super(PBS, self).__init__(agent)
        self.logger = TaskLogger(self.agent.master.work_dir, 'MAIN-SERVER').get_logger('PBS')

    def create_cmd(self):
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
        if self.cmd_data is None:
            self.cmd_data = self.create_cmd()
        if self.submit_times == 11:
            self.logger.error('PBS: %s -- delivery failed')
            return None

        output = os.popen('qsub %s' % self.cmd_data)
        self.submit_times += 1
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


class NOHUP(ABSClusterManager):
    """
        NOHUP task
    """

    def __init__(self, agent):
        super(NOHUP, self).__init__(agent)
        self.logger = TaskLogger(self.agent.master.work_dir, 'MAIN-SERVER').get_logger('PBS')

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
        if self.cmd_data is None:
            self.cmd_data = self.create_cmd()

        output = os.popen(self.cmd_data)
        self.submit_times += 1
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

    def check_state(self):
        """
        check status of current task

        :return: bool
        """
        return_status = False
        if self.id is not None:
            try:
                process = Process(self.id)
                return_status = True
            except NoSuchProcess:
                pass
            except (ZombieProcess, AccessDenied):
                return_status = True

        return return_status