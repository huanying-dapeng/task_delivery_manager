#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2019/5/4 12:28
@file    : master.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import csv
import os
import re
from collections import defaultdict

from gevent import Greenlet, event, joinall, sleep

from conf.settings import BIN_DIR, SERVER_PORT, SERVER_HOST
from core.params_parser import ParamsParser
from core.server import Server, ServerWorker
from .main_log import TaskLogger


class Master(dict):
    def __init__(self, params_obj: ParamsParser):
        super(Master, self).__init__()
        self._params_obj = params_obj
        self.logger = TaskLogger(self._params_obj.out_dir, 'MAIN-SERVER').get_logger('Master')
        self._work_dir = self._params_obj.out_dir
        self.server = None
        self.endpoint = 'tcp://{}:{:.0f}'.format(SERVER_HOST, SERVER_PORT)

    @property
    def work_dir(self):
        return self._work_dir

    def parser_params(self):
        tmp_dic = defaultdict(list)
        with open(self._params_obj.cmd_conf) as in_handler:
            for name, cmd, cpu, mem, relys in csv.reader(in_handler, delimiter='\t'):
                if name in self:
                    self.logger.error('the cmd name must not be repeated [%s is repeated]' % name)
                    raise KeyError('the cmd name must not be repeated [%s is repeated]' % name)
                self[name] = Agent(worker_id=name, cmd=cmd, master=self, cpu=cpu, mem=mem)
                relys = [i.strip() for i in relys.strip().split(',') if i]
                tmp_dic[name].extend(relys)
            for k, vs in tmp_dic.items():
                async_result = event.AsyncResult()
                k_agent = self[k]
                vs_agents = [self[i] for i in vs]
                k_agent.add_relys(async_result)
                for v_agent in vs_agents:
                    v_agent.add_relied(async_result)

    def stop(self):
        self.server.close()

    def run(self):
        self.server = Server(bind_obj=ServerWorker(self))
        self.server.start()
        agents = self.values()
        _ = [agent.start() for agent in agents]
        joinall([self.server, *agents])


class Agent(Greenlet):
    def __init__(self, worker_id, cmd, master: Master, cpu=2, mem='5G'):
        super(Agent, self).__init__()
        self.master = master
        self._relying_workers = set()
        self._relied_workers = set()
        self.__is_running = False
        self.__is_end = False
        self.__is_start = False
        self.__status = 'wait'
        self.__worker_id = worker_id
        self.__cmd = cmd
        self.name = self.worker_id
        self.__cpu = cpu
        self.__men = mem
        self.cmd_obj = None

    def _type_check(self, *w_events):
        for w_event in w_events:
            if not isinstance(w_event, event.AsyncResult):
                raise TypeError('relys must be event.AsyncResult')
        return True

    def add_relys(self, *w_events):
        self._type_check(*w_events)
        self._relying_workers.update(w_events)

    def add_relied(self, *w_events):
        self._type_check(*w_events)
        self._relied_workers.update(w_events)

    def _run(self):
        super(Agent, self)._run()
        self.is_start = True
        task_id = None
        if self.is_start is True:
            self.cmd_obj = CMDManager(self)
            task_id = self.cmd_obj.submit()
        if task_id is None:
            self.status = 'error'
            self.is_end = True

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
        msg = 1 if self.__status == 'end' else 0
        if value is True:
            _ = [i.set(msg) for i in self._relied_workers]
        self.__is_end = value

    @property
    def status(self):
        return self.__status

    @status.setter
    def status(self, value):
        self.__status = value

    @property
    def cmd(self):
        return self.__cmd

    def __bool__(self):
        return self.is_end


class PBS(object):
    """
        openPBS,
    """

    def __init__(self, agent: Agent):
        self.agent = agent
        self.logger = TaskLogger(self.agent.master.work_dir, 'MAIN-SERVER').get_logger('PBS')
        self.work_dir = self.agent.master.work_dir
        self.__submit_times = 0
        self.__pbs_file = None

    def create_file(self):
        """
        create PBS task file

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
            f.write("%s -i %s -e %s -c %s\n" % (script, self.agent.name, self.agent.master.endpoint, self.agent.cmd))

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


class CMDManager(PBS):

    @property
    def job_id(self):
        return self.id



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
