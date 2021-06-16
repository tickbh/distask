import random
import time

from distask.locks.base import BaseLock
from distask import util
from redlock import RedLock
from redlock.lock import ReentrantRedLock


DEFAULT_RLLOCK_NAME = "default_distributed_lock"

class RLLock(BaseLock):
    _lock = None
    def __init__(self, **kwargs):
        if kwargs.pop("reentrant", None):
            self._lock = ReentrantRedLock(DEFAULT_RLLOCK_NAME, **kwargs)
        else:
            self._lock = RedLock(DEFAULT_RLLOCK_NAME, **kwargs)

    def lock(self, timeout: float = -1):
        if timeout == -1:
            return self._lock.acquire()
        now = util.micro_now()
        while util.micro_now() - now < timeout:
            if self._lock.acquire():
                return True
            time.sleep(random.uniform(0, util.RETRY_DELAY/1000))
        return False

    def unlock(self):
        return self._lock.release()
