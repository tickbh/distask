import logging
import time, os, sys
from datetime import date, datetime, timezone
from os import PRIO_PGRP

from pymongo.mongo_client import MongoClient

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print("base_path ==", base_path)
sys.path.append(base_path)
sys.path.append(base_path + '/..')
sys.path.append(os.pardir)  # 为了导入父目录的文件而进行的设定

from distask import create_scheduler, task, register_job
from distask import util
from distask.datastores.mongodb import MongoDataStore
from distask.events import EVENT_SCHEDULER_START
from distask.locks.rl_lock import RLLock
from distask.schedulers.background import BackgroundScheduler
from distask.schedulers.base import Scheduler
from distask.serializers.json import JSONSerializer
from distask.serializers.pickle import PickleSerializer
from distask.tiggers.interval import IntervalTigger


def test00(times, aa=None, bb=None, *args):
    print("test0 ======================")
    time.sleep(0.1)
    # return True

def test_exception(times, aa=None, bb=None, *args):
    print("test0 ======================")
    a = 1 / 0
    # return True

def testcron(times, aa=None, bb=None):
    print("cron====================== ", util.time_now())

    # exit(0)
logging.basicConfig(level=logging.DEBUG)
tigger = IntervalTigger(seconds=100)

# import re
# s = "ssss:aaaa:_main_.test"
# res = re.match(r'^(\w+):(\w+):(\w.*)$',s)
# print(res)
# print(res.groups())
# exit(0)
# job = task.Job()

# serialize = PickleSerializer()
serialize = JSONSerializer()
client = MongoClient("mongodb://admin:123456@192.168.99.27:27017")
mongodb = MongoDataStore(client, serializer=serialize)
# connection_details=[
#     {'host': '192.168.99.27', 'port': 6379, 'db': 0},
# ]
connection_details=[
    {'host': '127.0.0.1', 'port': 6379, 'db': 0},
]
lock = RLLock(reentrant=True, connection_details=connection_details, ttl=10_000)
# scheduler = Scheduler(store=mongodb, lock=lock, group="", limit=1, maxwait=5)
# scheduler = Scheduler(store=mongodb, lock=lock, groups=['test'], limit=1, maxwait=5)
client_data = {
    "t": "mongo",
    "args": ["mongodb://admin:123456@192.168.99.27:27017"]
}
client_data = {
    't': 'redis',
    # 'args': ["redis://127.0.0.1:6379/0"],
    "host":'127.0.0.1', 
    'port':6379,
    'db':0, 
    # 'decode_responses':True
}
lock_data = {
    "t": "rllock",
    "reentrant":True, 
    "connection_details":connection_details, 
    "ttl":10_000
}
scheduler = create_scheduler(client_data, lock_data, serialize="pickle", groups=['test'], limit=1, maxwait=5)

@register_job(scheduler, "interval", args=(12, 123), group="test", subgroup="ssss", seconds=3)
def test1(times, aa=None, bb=None, *args):
    print("test111---------------------", util.time_now())

job = task.Job(test00, "interval", (12, 123), group="11", subgroup="", seconds=3)
scheduler.add_job(job)

job = task.Job(test_exception, "interval", (12, 123), seconds=3)
scheduler.add_job(job)

def job_execute(event):

    if event.code == EVENT_SCHEDULER_START:
        print("start success")
scheduler.add_listener(job_execute, EVENT_SCHEDULER_START)
scheduler.start()
while True:
    print("sleep!!!!!!!")
    time.sleep(5)
scheduler.shutdown()
# jobs = scheduler._get_jobs()
# print(jobs)
# for job in jobs:
#     if job.func:
#         tiggers = job.tigger.get_next_time(job.next_time * 1000)
#         print("tiggers len ==", len(tiggers))
#         print(job.tigger)
#         job.func(tiggers, *job.args)
