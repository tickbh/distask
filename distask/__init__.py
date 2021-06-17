
from distask.serializers.pickle import PickleSerializer
from distask.task import Job

from distask.schedulers.base import Scheduler
from distask.schedulers.background import BackgroundScheduler

def register_job(scheduler: Scheduler, *args, **kwargs) -> callable:
    def wrapper_register_job(func):
        job = Job(func, *args, **kwargs)
        scheduler.add_job(job)
        return func
    return wrapper_register_job