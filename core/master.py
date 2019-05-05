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
import time
from collections import defaultdict

from gevent import Greenlet, event, joinall, sleep

from conf.settings import (
    SERVER_PORT, SERVER_HOST, CLUSTER_NAME, INTERVAL_TIME, SUPER_ABNORMAL_INTERVAL
)
from core import job_schedule
from core.params_parser import ParamsParser
from core.server import Server, ServerWorker
from .main_log import TaskLogger


class Master(dict):
    """
    task management and scheduling
    """
    def __init__(self, params_obj: ParamsParser):
        super(Master, self).__init__()
        self._params_obj = params_obj
        self.logger = TaskLogger(self._params_obj.out_dir, 'MAIN-SERVER').get_logger('Master')
        self._work_dir = self._params_obj.out_dir
        self.server = None
        self.endpoint = 'tcp://{}:{:.0f}'.format(SERVER_HOST, SERVER_PORT)
        self.main_process = os.getpid()

    @property
    def work_dir(self):
        return self._work_dir

    def parser_params(self):
        tmp_dic = defaultdict(list)
        with open(self._params_obj.cmd_conf) as in_handler:
            for name, cpu, mem, relys, cmd in csv.reader(in_handler, delimiter='\t'):
                if name in self:
                    self.logger.error('the cmd name must not be repeated [%s is repeated]' % name)
                    raise KeyError('the cmd name must not be repeated [%s is repeated]' % name)
                self[name] = Agent(worker_id=name, cmd=cmd, master=self, cpu=cpu, mem=mem)
                relys = [i.strip() for i in relys.strip().split(',') if i]
                tmp_dic[name].extend(relys)
            for k, vs in tmp_dic.items():
                if len(vs) == 0:
                    continue
                async_result = event.AsyncResult()
                k_agent = self[k]
                vs_agents = [self[i] for i in vs]
                k_agent.add_relys(async_result)
                for v_agent in vs_agents:
                    v_agent.add_relied(async_result)

    def stop(self):
        self.server.close()

    def run(self):

        self.logger.info('start workflow: [PID %s]' % self.main_process)
        self.server = Server(bind_obj=ServerWorker(self))
        self.logger.info('start Server')
        self.server.start()
        self.logger.info('start to parse conf file')
        self.parser_params()
        self.logger.info('complete conf file parse')
        agents = self.values()
        # start all agent
        self.logger.info('start all workers')
        _ = [agent.start() for agent in agents]
        self.logger.info('start Agent Monitor')
        monitor = AgentMonitor(self)
        joinall(agents)
        for agent in agents:
            agent.get_end_signal()
        monitor.is_end = True
        self.logger.info('all workers were completed')
        self.logger.info('stop workflow: [PID %s]' % self.main_process)


class Agent(Greenlet):
    """
    the agent of remote worker in server
        1. keep the markers of time and status
        2. submit task
    """
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
        self.__task_id = None
        self.__cmd = cmd
        self.name = self.worker_id
        self.__cpu = cpu
        self.__men = mem
        self.cmd_obj = None
        self.__create_time = int(time.time())  # unit: S
        self.__start_run_time = None
        self.last_recv_time = None
        self.__end_signal = event.AsyncResult()

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
        """rewrite the method of Greenlet which will be called when the start method is invoked

        :return: None
        """
        super(Agent, self)._run()
        self.is_start = True
        task_id = None
        if self.is_start is True:
            self.cmd_obj = CMDManager(self)
            task_id = self.cmd_obj.submit()
            self.master.logger.debug(
                'Task: %s has submitted successfully, ID: %s, Task Type: %s' % (self.name, task_id, CLUSTER_NAME))
        if task_id is None:
            self.status = 'error'
            self.is_end = True
        else:
            self.__task_id = task_id
        self.__start_run_time = int(time.time())

    def get_resource(self):
        return self.__cpu, self.__men

    @property
    def worker_id(self):
        return self.__worker_id

    @property
    def create_time(self):
        return self.__create_time

    @property
    def start_time(self):
        return self.__start_run_time

    @property
    def is_start(self):
        return self.__is_start

    @is_start.setter
    def is_start(self, value):
        """
        1. block until all relying workers are completed successfully
        2. our worker start to run

        :param value: bool
        :return:
        """
        assert isinstance(value, bool), 'value of is_start must be bool'
        for i in self._relying_workers:
            v = i.get()
            if v != 1:
                value = False
        if value is True:
            self.master.logger.debug('start submit %s task' % self.worker_id)
        else:
            self.master.logger.debug('%s receive error, and it will stop running')
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
        """set the end of worker and send start or stop signal to the relied workers

        :param value: bool
        :return:
        """
        assert isinstance(value, bool), 'value of is_running must be bool'
        # 1: represent this work is completed successfully
        # 0: represent this work is failed, and stop the relied works
        msg = 1 if self.__status == 'end' else 0
        if value is True:
            _ = [i.set(msg) for i in self._relied_workers]

        self.master.logger.debug('%s has finished' % self.worker_id)
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

    @property
    def task_id(self):
        return self.__task_id

    def __bool__(self):
        return self.is_end

    def set_end_signal(self):
        self.__end_signal.set(1)

    def get_end_signal(self):
        return self.__end_signal.get(block=True)


class CMDManager(getattr(job_schedule, CLUSTER_NAME)):
    """manager of job scheduling system's tasks
        Dynamically bind the parent class through reflection
    """
    @property
    def job_id(self):
        return self.id


class AgentMonitor(Greenlet):
    def __init__(self, master: Master):
        super(AgentMonitor, self).__init__()
        self._master = master
        self.__logger = TaskLogger(self._master.work_dir, 'MAIN-SERVER').get_logger('Agent-Monitor')
        self.is_end = False

    def monitor(self):
        now_time = int(time.time())
        agents = self._master.values()
        for agent in agents:
            start_time = agent.start_time
            last_recv_time = agent.last_recv_time

            if start_time is None and last_recv_time is None:
                # solve the task which was not submitted 10 days later
                if (now_time - agent.create_time) > SUPER_ABNORMAL_INTERVAL:
                    agent.status = 'error'
                    agent.is_end = True
                continue
            elif last_recv_time is None:
                interval_time = now_time - start_time
            else:
                interval_time = now_time - last_recv_time

            # solve the submitted task which was received any information from the remote worker
            if interval_time > (INTERVAL_TIME * 10):
                status = agent.cmd_obj.check_state()
                set_end = False
                if status is False:  # the task has stopped but not receive end info
                    set_end = True
                else:
                    if interval_time > (INTERVAL_TIME * 20):
                        agent.cmd_obj.delete()
                        set_end = True
                if set_end:
                    agent.status = 'error'
                    agent.is_end = True

    def _run(self):
        sleep(3)
        while True:
            sleep(INTERVAL_TIME)
            self.monitor()
            if self.is_end:
                break


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
