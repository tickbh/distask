

a distribute task scheduler

# example

```python
from distask import create_scheduler, task, register_job
client_data = {
    "t": "mongo",
    "args": ["mongodb://admin:123456@127.0.0.1:27017"]
}
lock_data = {
    "t": "rllock",
    "reentrant":True, 
    "connection_details":connection_details, 
    "ttl":10_000
}
scheduler = create_scheduler(client_data, lock_data, serialize="json", groups=['test'], limit=1, maxwait=5)

@register_job(scheduler, "interval", args=(12, 123), group="test", subgroup="ssss", seconds=3)
def test1(times, aa=None, bb=None, *args):
    print("test---------------------", util.time_now())

```

because pickle not support lamba function, if you choose serialize type 'pickle' please install 'dill'