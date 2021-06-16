from abc import ABCMeta, abstractmethod
from base64 import b64decode, b64encode
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from re import sub
from typing import (Any, Callable, Dict, FrozenSet, Iterable, Iterator, List,
                    Optional, Set, Type)
from uuid import UUID, uuid4

from distask.tiggers.base import Tigger
from distask.tiggers.cron import CronTrigger
from distask.tiggers.delay import DelayTigger
from distask.tiggers.interval import IntervalTigger


class DeserializationError(AttributeError):
    """Raised when a serializer fails to deserialize the given object."""

@dataclass
class Job:
    job_id: str
    next_time: int
    group: str
    subgroup: str
    tigger: Tigger = field(compare=False)
    func: Callable = field(compare=False)
    args: tuple = field(compare=False)
    status_last_time: int
    deal_max_time: int = field(init=False, default=60_000)
    close_now: bool = field(init=False, default=False)

    def __init__(self, tigger, func, args=None, job_id=None, next_time=None, group='', subgroup='', status_last_time=0, **kwargs):
        
        if tigger == "interval":
            self.tigger = IntervalTigger(**kwargs)
        elif tigger == "delay":
            self.tigger = DelayTigger(**kwargs)
        elif tigger == "cron":
            self.tigger = CronTrigger(**kwargs)
        elif issubclass(tigger, Tigger):
            self.tigger = tigger
        else:
            raise AttributeError("unknow target({})".format(tigger))

        only_once = self.tigger.is_only_once()
        self.func = func
        self.args = args
        if job_id == None:
            if only_once:
                self.job_id = uuid4()
            else:
                self.job_id = f"{func.__module__}.{func.__name__}"
        else:
            self.job_id = job_id

        self.next_time = self.tigger.get_next_time()[0]
        self.group = group
        self.subgroup = subgroup
        self.status_last_time = status_last_time

    def call_func(self):
        tiggers = self.tigger.get_next_time(self.next_time)
        is_close = self.func(tiggers, *self.args, self.group, self.subgroup)
        return tiggers[-1], is_close

    def is_only_once(self):
        return self.tigger.is_only_once()

    def __getstate__(self):  
        """Return state values to be pickled."""
        return (self.job_id, self.next_time, self.group, self.subgroup, self.tigger, self.func, self.args, self.status_last_time)  

    def __setstate__(self, state):
        """Restore state from the unpickled state values."""  
        self.job_id, self.next_time, self.group, self.subgroup, self.tigger, self.func, self.args, self.status_last_time = state
