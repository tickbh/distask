from abc import ABC,abstractmethod,abstractproperty
from contextlib import contextmanager 

class LockError(Exception):
    pass

class BaseLock(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def lock(self, timeout: float = None, blocking=False):
        pass

    @abstractmethod
    def unlock(self):
        pass

    @contextmanager
    def create_lock(self, timeout: float = None, blocking=False):
        succ = self.lock(timeout)
        yield succ
        if succ:
            self.unlock()