import os, sys
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_path)
sys.path.append(base_path + '/..')
sys.path.append(os.pardir)  # 为了导入父目录的文件而进行的设定


import logging
logging.basicConfig(level=logging.DEBUG)

import time
from distask import create_scheduler, register_job
from distask import util
from distask.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_SCHEDULER_START

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

# lock_data = {
#     "t": "zklock",
#     "hosts":['192.168.99.27:2181'], 
# }


scheduler = create_scheduler(client_data, lock_data, serialize="pickle", limit=1, maxwait=5)

@register_job(scheduler, "interval", (), group="11", subgroup="", seconds=3)
def test_exception(times, *args, **kwargs):
    print("test0 ======================")
    # time.sleep(62)
    a = 1 / 0

@register_job(scheduler, 'cron', (12), hour='0,6,12,18', minute=0, timezone='Asia/Shanghai')
def testcron(times, a, *arg, **kwargs):
    assert a == 12, "arg is ok"
    print("cron====================== ", util.time_now())


@register_job(scheduler, "interval", (), group="11", subgroup="", seconds=3)
def test000(times, *args, **kwargs):
    print("test0 ======================")
    time.sleep(0.1)

def scheduler_start(event):
    if event.code == EVENT_SCHEDULER_START:
        print("start success")
scheduler.add_listener(scheduler_start, EVENT_SCHEDULER_START)

def job_execute(event):
    if event.code == EVENT_JOB_ERROR:
        print("event {} error".format(event.job_id))
        print("exec_type: {}".format(event.exec_type))
        print("exec_value: {}".format(event.exec_value))
        print("traceback: {}".format(event.traceback))
        import traceback
        traceback.print_tb(event.traceback)
    if event.code == EVENT_JOB_EXECUTED:
        print("event {} success".format(event.job_id))
scheduler.add_listener(job_execute, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
scheduler.start()

# def runFunctionCatchExceptions(func, *args, **kwargs):
#     try:
#         result = func(*args, **kwargs)
#     except Exception as message:
#         return ["exception", message]

#     return ["RESULT", result]


# def runFunctionWithTimeout(func, args=(), kwargs={}, timeout_duration=10, default=None):
#     import threading
#     class InterruptableThread(threading.Thread):
#         def __init__(self):
#             threading.Thread.__init__(self)
#             self.result = default
#         def run(self):
#             self.result = runFunctionCatchExceptions(func, *args, **kwargs)
#     it = InterruptableThread()
#     it.start()
#     it.join(timeout_duration)
#     if it.is_alive():
#         return default

#     if it.result[0] == "exception":
#         raise it.result[1]

#     return it.result[1]

# def remote_calculate(aaa):
#     print(aaa)
#     i = 0
#     while True:
#         print(i)
#         i = i + 1
#         time.sleep(1)

# result = runFunctionWithTimeout(remote_calculate, (1,), timeout_duration=5)
# print("result ========== ", result)
