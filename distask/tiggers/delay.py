import time

from distask.tiggers.base import Tigger

from distask import util

class DelayTigger(Tigger):
    '''每隔相同的时间会触发'''

    def __init__(self, microseconds = None, seconds=None, minutes=None, hours=None, date=None):
        self.microseconds = 0
        if microseconds: self.microseconds = microseconds 
        if seconds: self.microseconds = seconds * 1000 + self.microseconds
        if minutes: self.microseconds = minutes * 60_000 + self.microseconds
        if hours: self.microseconds = hours * 3600_000 + self.microseconds
        if date:
            self.microseconds = util.datetime_to_timestamp(date) * 1000 - util.micro_now()
        if not self.microseconds:
            raise AttributeError("not vaild interval")

    def get_next_time(self, pre = None, limit=None):
        now = util.micro_now()
        tiggers = [now + self.microseconds]
        return tiggers

    def __str__(self) -> str:
        return "delay {} microseconds".format(self.microseconds)

    def __getstate__(self):  
        """Return state values to be pickled."""
        return (self.microseconds)

    def __setstate__(self, state):
        """Restore state from the unpickled state values."""  
        self.microseconds = state

    def is_only_once(self):
        return True