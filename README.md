

a distribute task scheduler

# example

```python
import time
from distask import create_scheduler, register_job
from distask import util
from distask.events import EVENT_SCHEDULER_START

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

def job_execute(event):
    if event.code == EVENT_SCHEDULER_START:
        print("start success")
scheduler.add_listener(job_execute, EVENT_SCHEDULER_START)
scheduler.start()
```
