import unittest
from distask import util, register_job
from testcase.base import create_base_scheduler
from distask.task import Job

class TestStringMethods(unittest.TestCase):

    def test_empty_jobs(self):
        scheduler = create_base_scheduler()
        all_jobs = scheduler.get_all_jobs()
        self.assertTrue(len(all_jobs) == 0)

        scheduler = create_base_scheduler(db="redis")
        all_jobs = scheduler.get_all_jobs()
        self.assertTrue(len(all_jobs) == 0)
    
    def local_lamaba_jobs(times, *args, scheduler=None):
        scheduler.shutdown()

    def test_jobs_run(self):
        scheduler = create_base_scheduler()
        job = Job(TestStringMethods.local_lamaba_jobs, "interval", (), seconds=1);
        scheduler.add_job(job)
        scheduler.start()
        self.assertTrue(True)

    def test_group_job(self):
        scheduler = create_base_scheduler(groups=["aaa", "bbb"], subgroups=["ccc", "ddd"])

        def _add_job(group, subgroup, now_jobs):
            job = Job(TestStringMethods.local_lamaba_jobs, "delay", (), group=group, subgroup=subgroup, seconds=1);
            scheduler.add_job(job)
            all_jobs = scheduler.get_all_jobs()
            self.assertTrue(len(all_jobs) == now_jobs)

        _add_job("aaa", "", 0)
        _add_job("aaa", "ccc", 1)
        _add_job("aaa", "ccc", 2)
        _add_job("ccc", "ccc", 2)
        _add_job("", "ccc", 2)
        _add_job("aaa", "ddd", 3)
        _add_job("bbb", "ddd", 4)
        _add_job("bbb", "eee", 4)


if __name__ == '__main__':
    unittest.main()