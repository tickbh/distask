import unittest
from distask import util, CronTigger, IntervalTigger, DelayTigger
from testcase.base import create_base_scheduler

class TestStringMethods(unittest.TestCase):

    def test_jobs(self):
        scheduler = create_base_scheduler()

        


if __name__ == '__main__':
    unittest.main()