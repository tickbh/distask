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
        lock_key = kwargs.pop("lock_key", DEFAULT_RLLOCK_NAME)
        if kwargs.pop("reentrant", None):
            self._lock = ReentrantRedLock(lock_key, **kwargs)
        else:
            self._lock = RedLock(lock_key, **kwargs)

    def lock(self, timeout: float = None, blocking=False):
        if not timeout:
            return self._lock.acquire()
        now = util.micro_now()
        while util.micro_now() - now < timeout:
            if self._lock.acquire():
                return True
            time.sleep(random.uniform(0, util.RETRY_DELAY/1000))
        return False

    def unlock(self):
        return self._lock.release()
