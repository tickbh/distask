from distask.task import Job
import unittest
from distask import util, register_job
from testcase.base import create_base_scheduler

class TestStringMethods(unittest.TestCase):

    def test_empty_jobs(self):
        scheduler = create_base_scheduler()
        all_jobs = scheduler.get_all_jobs()
        self.assertTrue(len(all_jobs) == 0)

        scheduler = create_base_scheduler(db="redis")
        all_jobs = scheduler.get_all_jobs()
        self.assertTrue(len(all_jobs) == 0)
    
    def local_lamaba_jobs(times, *args, scheduler=None):
        print("local_lamaba_jobs called!!")
        scheduler.shutdown()

    def test_jobs_run(self):
        scheduler = create_base_scheduler()
        job = Job(TestStringMethods.local_lamaba_jobs, "interval", (), seconds=3);
        scheduler.add_job(job)
        scheduler.start()


if __name__ == '__main__':
    unittest.main()