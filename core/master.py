#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/4 12:28
@file    : master.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import os
import re

from conf.settings import BIN_DIR
from core.params_parser import ParamsParser
from gevent import Greenlet, event, lock, spawn, joinall, getcurrent


class Master(object):
    def __init__(self, params_obj: ParamsParser):
        self._params_obj = params_obj
        self._work_dir = self._params_obj.out_dir

    @property
    def work_dir(self):
        return self._work_dir


class Agent(Greenlet):
    def __init__(self, worker_id, cmd, master: Master, cpu=2, mem='5G'):
        super(Agent, self).__init__()
        self.master = master
        self._relying_workers = []
        self._relied_workers = []
        self.__is_running = False
        self.__is_end = False
        self.__is_start = False
        self.__worker_id = worker_id
        self.__cmd = cmd
        self.name = self.worker_id
        self.__cpu = cpu
        self.__men = mem

    def _type_check(self, *w_events):
        for w_event in w_events:
            if not isinstance(w_event, event.AsyncResult):
                raise TypeError('relys must be event.AsyncResult')
        return True

    def add_relys(self, *w_events):
        self._type_check(*w_events)
        self._relying_workers.extend(w_events)

    def add_relied(self, *w_events):
        self._type_check(*w_events)
        self._relied_workers.extend(w_events)

    def _run(self):
        super(Agent, self)._run()
        self.is_start = True

    def get_resource(self):
        return self.__cpu, self.__men

    @property
    def worker_id(self):
        return self.__worker_id

    @property
    def is_start(self):
        return self.__is_start

    @is_start.setter
    def is_start(self, value):
        assert isinstance(value, bool), 'value of is_start must be bool'
        for i in self._relying_workers:
            v = i.get()
            if v != 1:
                value = False
        self.__is_start = value

    @property
    def is_running(self):
        return self.__is_running

    @is_running.setter
    def is_running(self, value):
        assert isinstance(value, bool), 'value of is_running must be bool'
        self.__is_running = value

    @property
    def is_end(self):
        return self.__is_end

    @is_end.setter
    def is_end(self, value):
        assert isinstance(value, bool), 'value of is_running must be bool'
        if value is True:
            _ = [i.set(1) for i in self._relied_workers]
        self.__is_end = value


class PBS(object):
    """
        openPBS任务调度系统,
        用于生产和管理PBS任务
        """

    def __init__(self, agent: Agent):
        self.agent = agent
        self.work_dir = self.agent.master.work_dir

    def create_file(self):
        """
        生成PBS脚本用于投递

        :return:
        """
        file_path = os.path.join(self.work_dir, self.agent.name + ".pbs")
        script = os.path.join(BIN_DIR + "cmd_runner.py")
        cpu, mem = self.agent.get_resource()
        with open(file_path, "w") as f:
            f.write("#PBS -N %s\n" % self.agent.name)
            f.write("#PBS -l nodes=1:ppn=%s\n" % cpu)
            f.write("#PBS -l mem=%s\n" % mem)
            f.write("#PBS -e %s" % self.agent.name + '.e')
            f.write("#PBS -o %s" % self.agent.name + '.o')
            f.write("#PBS -d %s\n" % self.agent.master.work_dir)
            f.write("cd %s\n\n" % self.agent.master.work_dir)
            f.write("%s %s\n" % (script, self.agent.name))

        return file_path

    def submit(self):
        """
        提交PBS任务,并返回Jobid

        :return: jobid
        """
        super(PBS, self).submit()
        pbs_file = self.create_file()
        output = os.popen('ssh -o GSSAPIAuthentication=no %s "/opt/torque/bin/qsub %s"' % (self.master_ip, pbs_file))
        text = output.read()
        if re.match(r'Maximum number', text):
            self.agent.logger.warn("到达最大任务书，30秒后尝试再次投递!")
            gevent.sleep(30)
            self.submit()
        else:
            m = re.search(r'(\d+)\..*', text)
            if m:
                self.id = m.group(1)
                return self.id
            else:
                self.agent.logger.warn("任务投递系统出现错误:%s，30秒后尝试再次投递!\n" % output)
                gevent.sleep(30)
                self.submit()

    def delete(self):
        """
        删除当前任务

        :return: None
        """

        if self.check_state():
            os.system('qdel -p %s' % self.id)

    def check_state(self):
        """
        检测任务状态

        :return: string 返回任务状态代码 如果任务不存在 则返回False
        """
        output = os.popen('qstat -f %s' % self.id)
        text = output.read()
        m = re.search(r"job_state = (\w+)", text)
        if m:
            return m.group(1)
        else:
            return False


class CMDManager(object):
    pass



if __name__ == '__main__':
    # def run():
    #     c = CMDManager()
    #
    #     print(id(c), getcurrent().name)
    #
    # a = [spawn(run), spawn(run), spawn(run)]
    # d = spawn(run)
    # d.name = '============='
    # a.append(d)
    # joinall(a)
    c = Agent('1', '====')
    c1 = Agent('2', '====')
    c.start()
    c1.start()

    joinall((c, c1))
    # c1 = CMDManager()
    # print(id(c), id(c1))
