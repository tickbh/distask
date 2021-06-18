from dataclasses import dataclass
from datetime import datetime, timedelta
import unittest
from distask import util, CronTigger, IntervalTigger, DelayTigger
from testcase.base import create_base_scheduler

class TestStringMethods(unittest.TestCase):

    def test_interval(self):
        now = util.micro_now()
        interval = IntervalTigger(microseconds=10)
        tiggers = interval.get_next_time()
        self.assertTrue(len(tiggers) == 1)
        self.assertTrue(tiggers[-1] - now >= 10)

        tiggers = interval.get_next_time(now - 10 * 5.9)
        self.assertTrue(len(tiggers) == 6)

        now = util.micro_now()
        interval = IntervalTigger(seconds=10)
        tiggers = interval.get_next_time()
        self.assertTrue(len(tiggers) == 1)
        self.assertTrue(tiggers[-1] - now >= 10_000)

    def test_delay(self):
        now = util.micro_now()
        interval = DelayTigger(microseconds=10)
        tiggers = interval.get_next_time()
        self.assertTrue(len(tiggers) == 1)
        self.assertTrue(tiggers[-1] - now >= 10)

        tiggers = interval.get_next_time(now - 10 * 5.9)
        self.assertTrue(len(tiggers) == 1)
        self.assertTrue(tiggers[-1] - now >= 10)

        now = util.micro_now()
        date = datetime.now() + timedelta(seconds=10)
        interval = DelayTigger(date=date)
        tiggers = interval.get_next_time()
        self.assertTrue(len(tiggers) == 1)
        self.assertTrue(tiggers[-1] - now >= 10_000)
        self.assertTrue(tiggers[-1] - now <= 10_100)
        


if __name__ == '__main__':
    unittest.main()