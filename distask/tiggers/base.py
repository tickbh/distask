
from abc import ABC,abstractmethod

DEFAULT_MAX_JOB = 10

class Tigger(ABC):
    ''''''

    def __init__(self) -> None:
        self._events = []

    @abstractmethod
    def get_next_time(self, pre):
        pass

    def is_only_once(self):
        return False