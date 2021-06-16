from abc import ABC,abstractmethod,abstractproperty
from contextlib import contextmanager 

class LockError(Exception):
    pass

class BaseLock(ABC):
    _lock = 1
    @abstractmethod
    def __init__(self):
        self._lock = 2
        pass

    @abstractmethod
    def lock(self, timeout: float = -1):
        pass

    @abstractmethod
    def unlock(self):
        pass

    @contextmanager
    def create_lock(self, timeout: float = -1):
        succ = self.lock(timeout)
        yield succ
        if succ:
            self.unlock()