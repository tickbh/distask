import time

from distask import util
from distask.tiggers.base import Tigger


class IntervalTigger(Tigger):
    '''每隔相同的时间会触发'''

    def __init__(self, microseconds = None, seconds=None, minutes=None, hours=None, days=None):
        self.microseconds = 0
        if microseconds: self.microseconds = microseconds 
        if seconds: self.microseconds = seconds * 1000 + self.microseconds
        if minutes: self.microseconds = minutes * 60_000 + self.microseconds
        if hours: self.microseconds = hours * 3600_000 + self.microseconds
        if days: self.microseconds = days * 86400_000 + self.microseconds
        if not self.microseconds:
            raise AttributeError("not vaild interval")

    def get_next_time(self, pre = None, limit=None):
        now = util.micro_now()
        tiggers = []
        if not pre or pre >= now:
            tiggers.append(now + self.microseconds)
            return tiggers
        for t in range(pre, now, self.microseconds):
            tiggers.append(t + self.microseconds)
        return tiggers

    def __str__(self) -> str:
        return "interval {} microseconds".format(self.microseconds)

    def __getstate__(self):  
        """Return state values to be pickled."""
        return (self.microseconds)

    def __setstate__(self, state):
        """Restore state from the unpickled state values."""  
        self.microseconds = state
