from abc import ABC,abstractmethod
from typing import List

class DataStore(ABC):
    ''''''

    @abstractmethod
    def get_jobs(self, scheduler, now, limit=None) -> List:
        return None

    @abstractmethod
    def get_all_jobs(self, scheduler) -> List:
        return None
        
    @abstractmethod
    def modify_job(self, job, update):
        return None

    @abstractmethod
    def remove_job(self, job):
        return None
