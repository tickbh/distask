import logging
import time, os, sys
from datetime import date, datetime, timezone
from os import PRIO_PGRP

from pymongo.mongo_client import MongoClient
from pytz import UTC

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print("base_path ==", base_path)
sys.path.append(base_path)
sys.path.append(base_path + '/..')
sys.path.append(os.pardir)  # 为了导入父目录的文件而进行的设定

from distask import create_scheduler, task, register_job
from distask import util
from distask.events import EVENT_SCHEDULER_START


logging.basicConfig(level=logging.DEBUG)
enable_redis = False
client_data = {
    "t": "mongo",
    "client_args": {
        "host": "mongodb://admin:123456@192.168.99.27:27017"
    },
    "store_args": {
        "database": 'testcase_distask', 
        "schedules": 'schedules', 
        "jobs": 'jobs',
    }
}
if enable_redis:
    client_data = {
        't': 'redis',
        "host":'127.0.0.1', 
        'port':6379,
        'db':15, 
    }

connection_details=[
    {'host': '127.0.0.1', 'port': 6379, 'db': 15},
]
lock_data = {
    "t": "rllock",
    "reentrant":True, 
    "connection_details":connection_details, 
    "ttl":10_000
}
scheduler = create_scheduler(client_data, lock_data, serialize="pickle", limit=1, maxwait=5)

@register_job(scheduler, "interval", (), group="11", subgroup="", seconds=3)
def test_exception(times, *args, **kwargs):
    print("test0 ======================")
    a = 1 / 0

@register_job(scheduler, 'cron', (12), hour='0,6,12,18', minute=0, timezone='Asia/Shanghai')
def testcron(times, a, *arg, **kwargs):
    assert a == 12, "arg is ok"
    print("cron====================== ", util.time_now())


@register_job(scheduler, "interval", (), group="11", subgroup="", seconds=3)
def test00(times, *args, **kwargs):
    print("test0 ======================")
    time.sleep(0.1)
    # return True

def job_execute(event):

    if event.code == EVENT_SCHEDULER_START:
        print("start success")
scheduler.add_listener(job_execute, EVENT_SCHEDULER_START)
scheduler.start()
while True:
    print("sleep!!!!!!!")
    time.sleep(5)
