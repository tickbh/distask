import logging
import re
from typing import Optional

from distask import util
from distask.datastores.base import DataStore
from distask.serializers.base import Serializer
from distask.task import Job
from distask.util import bytes_to_int, bytes_to_str

try:
    from redis import Redis
except ImportError:
    raise ImportError('RedisDataStore requires redis installed')

class RedisDataStore(DataStore):
    '''job_id will be add group and subgroup'''
    def __init__(self, client: Redis, *, serializer: Optional[Serializer] = None,
                run_times_key: str = 'distask_times',
                jobs_key: str = 'distask_jobs', **kwargs):
            
        self._client = client
        self._serializer = serializer
        self._run_times_key = run_times_key
        self._jobs_key = jobs_key
        self._key_pattern = re.compile(r'^(\w*)::(\w*):(\w*):(\w.+)$')

    def _job_id_to_key(self, job_id):
        return self._jobs_key + "::" + job_id

    def _build_job_id(self, group, subgroup, job):
        return "{}:{}:{}".format(group, subgroup, job)

    def _build_id_key_by_job(self, job):
        job_id = self._job_id_to_key(self._build_job_id(job.group, job.subgroup, job.job_id))
        return job_id
    
    def get_jobs(self, scheduler, now, limit=None):
        job_ids = self._client.zrangebyscore(self._run_times_key, 0, now)
        if not job_ids:
            return []
        filter_jobs = []
        need_del_jobs = []
        has_execption = False
        for job_id in job_ids:
            job_id = bytes_to_str(job_id)
            match = self._key_pattern.match(job_id)
            if not match:
                need_del_jobs.append(job_id)
                continue
            _, group, subgroup, job = match.groups()
            if scheduler._groups and not group in scheduler._groups:
                continue

            if scheduler._subgroups and not subgroup in scheduler._subgroups:
                continue

            filter_jobs.append(job_id)

            if limit and limit > 0 and len(filter_jobs) >= limit:
                break
        jobs = []
        for job_id in filter_jobs:
            row = self._client.hgetall(job_id)
            try:
                jobs.append(self._reconstitute_job(row))
            except BaseException:
                logging.exception('Unable to restore job "%s" -- removing it', job_id)
                need_del_jobs.append(job_id)
                has_execption = True

        # Remove all the jobs we failed to restore
        if need_del_jobs:
            with self._client.pipeline() as pipe:
                pipe.delete(*need_del_jobs)
                pipe.zrem(self._run_times_key, *need_del_jobs)
                pipe.execute()
        return jobs, has_execption

    def _reconstitute_job(self, row):
        job = Job.__new__(Job)
        func_str, args = self._serializer.deserialize(row[b'call'])
        states = (
            bytes_to_str(row[b'job_id']), 
            bytes_to_int(row[b'next_time']), 
            bytes_to_str(row[b'group']), 
            bytes_to_str(row[b'subgroup']), 
            self._serializer.deserialize(row[b'tigger']),
            util.ref_to_obj(func_str), args, 
            bytes_to_int(row[b'status_last_time']),
        )
        job.__setstate__(states)
        return job

    def add_job(self, job):
        job_id = self._build_id_key_by_job(job)
        old = self._client.hgetall(job_id)
        if not old:
            document = {
                'job_id': job.job_id,
                'group': job.group,
                'subgroup': job.subgroup,
                'status_last_time': job.status_last_time,
                'next_time': job.next_time,
                'tigger': self._serializer.serialize(job.tigger),
                'call': self._serializer.serialize((job.get_func_str(), job.args)),
            }
            with self._client.pipeline() as pipe:
                pipe.multi()
                pipe.hmset(job_id, document)
                pipe.zadd(self._run_times_key, {job_id: job.next_time}) 
                pipe.execute()
        else:
            update = {
                'tigger': self._serializer.serialize(job.tigger),
                'call': self._serializer.serialize((job.get_func_str(), job.args)),
            }
            if job.next_time < bytes_to_int(old[b'next_time']):
                update['next_time'] = job.next_time
            
            with self._client.pipeline() as pipe:
                pipe.multi()
                pipe.hmset(job_id, update)
                if 'next_time' in update:
                    pipe.zadd(self._run_times_key, {job_id: update['next_time']}) 
                pipe.execute()

        return None

    def get_all_jobs(self, scheduler):
        return self.get_jobs(scheduler, util.micro_max())

    def modify_job(self, job, update):
        job_id = self._build_id_key_by_job(job)
        with self._client.pipeline() as pipe:
            pipe.multi()
            pipe.hmset(job_id, update)
            if 'next_time' in update:
                pipe.zadd(self._run_times_key, {job_id: update['next_time']}) 
            pipe.execute()

    def remove_job(self, job):
        job_id = self._build_id_key_by_job(job)
        
        with self._client.pipeline() as pipe:
            pipe.multi()
            pipe.delete(job_id)
            pipe.zrem(self._run_times_key, job_id) 
            pipe.execute()
        return None

    def record_job_exec(self, status, job, duration=0, runtimes=1, excetion=None):
        return None

    def clear_all_jobs_info(self):
        all_keys = self._client.keys(self._jobs_key + "*")
        with self._client.pipeline() as pipe:
            pipe.multi()
            if len(all_keys) > 0:
                pipe.delete(*all_keys)
            pipe.delete(self._run_times_key) 
            pipe.execute()