from distask.serializers.json import JSONSerializer
from distask.serializers.pickle import PickleSerializer
from distask.task import Job
from distask.locks.base import BaseLock
from distask.datastores.base import DataStore
from distask.schedulers.base import Scheduler
from distask.schedulers.background import BackgroundScheduler

def register_job(scheduler: Scheduler, *args, **kwargs) -> callable:
    def wrapper_register_job(func):
        job = Job(func, *args, **kwargs)
        scheduler.add_job(job)
        return func
    return wrapper_register_job

def create_scheduler(client_data, lock_data, serialize="json", **kwargs):
    serialize =  PickleSerializer() if serialize == "pickle" else JSONSerializer()
    client = None
    store = None
    if isinstance(client_data, DataStore):
        store = client_data
    else:
        cliet_type = client_data.pop("t")
        if cliet_type == "mongo":
            try:
                from pymongo import MongoClient
                from distask.datastores.mongodb import MongoDataStore
                args = client_data.pop("args", [])
                client = MongoClient(*args, **client_data)
                store = MongoDataStore(client, serializer=serialize)
            except:
                raise
        elif cliet_type == "redis":
            try:
                from redis import Redis
                from distask.datastores.redis import RedisDataStore
                args = client_data.pop("args", [])
                client = Redis(*args, **client_data)
                store = RedisDataStore(client, serializer=serialize, **client_data)
            except:
                raise
            
    assert store, "must store client exist"
    lock = None
    if isinstance(lock_data, BaseLock):
        lock = lock_data
    else:
        lock_type = lock_data.pop("t")
        if lock_type == "rllock":
            try:
                from distask.locks.rl_lock import RLLock
                args = lock_data.pop("args", [])
                lock = RLLock(*args, **lock_data)
            except:
                raise

    assert lock, "must lock exist"

    scheduler = Scheduler(store=store, lock=lock, **kwargs)
    return scheduler