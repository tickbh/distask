
import logging
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from threading import Event, RLock

from distask.events import (EVENT_ALL, EVENT_JOB_ADDED, EVENT_JOB_REMOVED,
                            EVENT_SCHEDULER_PAUSED, EVENT_SCHEDULER_RESUMED,
                            EVENT_SCHEDULER_SHUTDOWN, EVENT_SCHEDULER_START,
                            JobEvent, SchedulerEvent)
from distask.task import Job
from distask import util

DEFAULT_MAX_JOB = 10
DEFAULT_MAX_WAIT = 60

STATE_STOPPED = 0
STATE_RUNNING = 1
STATE_PAUSED = 2

class Scheduler(ABC):
    ''''''
    def __init__(self, store=None, lock=None, limit=DEFAULT_MAX_JOB, groups=[], subgroups=[], maxwait=DEFAULT_MAX_WAIT) -> None:
        self._store = store
        self._limit = limit
        self._lock = lock
        assert type(groups) == list, "groups must be list"
        assert type(subgroups) == list, "subgroups must be list"
        self._groups = groups
        self._subgroups = subgroups
        self._maxwait = maxwait
        self._jobs = []
        self._event = None

        self.state = STATE_STOPPED
        self._listeners_lock = RLock()
        self._listeners = []

    def _process_jobs(self):
        if self.state == STATE_PAUSED:
            logging.debug('Scheduler is paused -- not processing jobs')
            return None

        logging.debug('Looking for jobs to run')
        now = util.micro_now()
        next_wakeup_time = now + 6_000
        jobs = []

        with self._lock.create_lock() as succ:
            if not succ:
                return 0.1
            try:
                jobs = self._get_jobs(self._limit)
            except Exception as e:
                import traceback
                traceback.print_exc()
                logging.warning('get job occur error')
                pass
            
            # update task status
            for job in jobs:
                changes = {
                    "status_last_time": now + job.deal_max_time,
                }
                self._store.modify_job(job, changes)
        for job in jobs:
            try:
                job.next_time, job.close_now = job.call_func()
            except Exception as e:
                logging.warning("run job %s catch exception(%s)" % (job.job_id, e))

        with self._lock.create_lock(60_000) as succ:
            for job in jobs:
                if job.is_only_once() or job.next_time == 0 or job.close_now:
                    self._store.remove_job(job)
                else:
                    changes = {
                        "status_last_time": 0,
                        "next_time": job.next_time
                    }
                    next_wakeup_time = min(job.next_time, next_wakeup_time)
                    self._store.modify_job(job, changes)

        wait_microseconds = next_wakeup_time - now
        if (len(jobs) == self._limit and wait_microseconds > 60_000) or wait_microseconds < 0:
            wait_microseconds = 0
        return min(self._maxwait, wait_microseconds / 1000)

    def _get_jobs(self, limit=None):
        now = util.micro_now()
        jobs = self._store.get_jobs(self, now, limit)
        return jobs

    def get_all_jobs(self, limit=None):
        jobs = self._store.get_all_jobs(self)
        return jobs

    def add_job(self, job: Job):
        self._store.add_job(job)
        self._dispatch_event(JobEvent(EVENT_JOB_ADDED, job.job_id))

    def remove_job(self, job):
        remove_job = self._store.remove_job({
            'job_id': job.job_id,
            'group': job.group,
            'subgroup': job.subgroup
        })

        event = JobEvent(EVENT_JOB_REMOVED, job['job_id'])
        self._dispatch_event(event)

        logging.info('Removed job %s', job['job_id'])

    def start(self, paused=False, ready=True):
        if self._event is None or self._event.is_set():
            self._event = Event()

        self.state = STATE_PAUSED if paused else STATE_RUNNING
        self._dispatch_event(SchedulerEvent(EVENT_SCHEDULER_START))
        if ready:
            self._main_loop()

    def shutdown(self, wait=True):
        self._event.set()
        self._dispatch_event(SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN))


    def pause(self):
        if self.state == STATE_STOPPED:
            return
        elif self.state == STATE_RUNNING:
            self.state = STATE_PAUSED
            logging.info('Paused scheduler job processing')
            self._dispatch_event(SchedulerEvent(EVENT_SCHEDULER_PAUSED))

    def resume(self):
        if self.state == STATE_STOPPED:
            return
        elif self.state == STATE_PAUSED:
            self.state = STATE_RUNNING
            logging.info('Resumed scheduler job processing')
            self._dispatch_event(SchedulerEvent(EVENT_SCHEDULER_RESUMED))
            self.wakeup()

    def _main_loop(self):
        wait_seconds = 0
        while self.state != STATE_STOPPED:
            self._event.wait(wait_seconds)
            self._event.clear()
            wait_seconds = self._process_jobs()

    def wakeup(self):
        self._event.set()


    def add_listener(self, callback, mask=EVENT_ALL):
        with self._listeners_lock:
            self._listeners.append((callback, mask))

    def remove_listener(self, callback):
        with self._listeners_lock:
            for i, (cb, _) in enumerate(self._listeners):
                if callback == cb:
                    del self._listeners[i]

    def _dispatch_event(self, event):
        with self._listeners_lock:
            listeners = tuple(self._listeners)

        for cb, mask in listeners:
            if event.code & mask:
                try:
                    cb(event)
                except BaseException:
                    logging.exception('Error notifying listener')
