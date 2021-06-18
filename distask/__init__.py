from distask.serializers.json import JSONSerializer
from distask.serializers.pickle import PickleSerializer
from distask.locks.base import BaseLock
from distask.datastores.base import DataStore
from distask.schedulers.base import Scheduler
from distask.schedulers.background import BackgroundScheduler
from distask.tiggers.cron import CronTigger
from distask.tiggers.delay import DelayTigger
from distask.tiggers.interval import IntervalTigger
from distask.task import Job

def register_job(scheduler: Scheduler, *args, **kwargs) -> callable:
    def wrapper_register_job(func):
        job = Job(func, *args, **kwargs)
        scheduler.add_job(job)
        return func
    return wrapper_register_job

def create_scheduler(client_data, lock_data, serialize="pickle", backgroud=False, **kwargs):
    serialize =  JSONSerializer() if serialize == "json" else PickleSerializer()
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
                client_args = client_data.pop("client_args", {})
                store_args = client_data.pop("store_args", {})
                client = MongoClient(**client_args)
                store = MongoDataStore(client, serializer=serialize, **store_args)
            except:
                raise
        elif cliet_type == "redis":
            try:
                from redis import Redis
                from distask.datastores.redis import RedisDataStore
                client_args = client_data.pop("client_args", {})
                store_args = client_data.pop("store_args", {})
                client = Redis(*client_args)
                store = RedisDataStore(client, serializer=serialize, **store_args)
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
        if lock_type == "zklock":
            try:
                from distask.locks.zk_lock import ZKLock
                args = lock_data.pop("args", [])
                lock = ZKLock(*args, **lock_data)
            except:
                raise

    assert lock, "must lock exist"

    if backgroud:
        scheduler = BackgroundScheduler(store=store, lock=lock, **kwargs)
    else:
        scheduler = Scheduler(store=store, lock=lock, **kwargs)
    return scheduler