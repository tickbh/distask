import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from traceback import print_exc
from typing import List, Optional

import pymongo
from distask import util
from distask.datastores.base import DataStore
from distask.serializers.base import Serializer
from distask.task import DeserializationError, Job
from pymongo import ASCENDING, DeleteOne, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError


class MongoDataStore(DataStore):
    def __init__(self, client: MongoClient, *, serializer: Optional[Serializer] = None,
                database: str = 'distask', 
                schedules: str = 'schedules',
                jobs: str = 'jobs',
                store_status: bool = True, **kwargs):
        self._client = client
        self._database = database
        self._serializer = serializer
        self._schedules = client[self._database][schedules]
        self._jobs = client[self._database][jobs]
        self._store_status = store_status

        self._create_index()

    def _create_index(self):
        self._jobs.create_index(
        [
            ("job_id", pymongo.ASCENDING),
            ("group", pymongo.ASCENDING),
            ("subgroup", pymongo.ASCENDING),
        ], unique=True)

        self._jobs.create_index(
        [
            ("next_time", pymongo.ASCENDING),
        ])

    def _reconstitute_job(self, row):
        job = Job.__new__(Job)
        func_str, args = self._serializer.deserialize(row['call'])
        states = (row['job_id'], row['next_time'], row['group'], row['subgroup'], 
            self._serializer.deserialize(row['tigger']), util.ref_to_obj(func_str), args, row['status_last_time'])
        job.__setstate__(states)
        return job


    def get_all_jobs(self, scheduler) -> List:
        return self.get_jobs(scheduler, util.micro_max())

    def get_jobs(self, scheduler, now, limit=None) -> List:
        jobs: List[Job] = []
        has_execption = False
        filters = {"next_time": {"$lt": now}, "status_last_time": {"$lt": now}}
        if len(scheduler._groups): filters["group"] = {"$in": scheduler._groups}
        if len(scheduler._subgroups): filters["subgroup"] = {"$in": scheduler._subgroups}
        
        rows = self._jobs.find(filters).sort([('next_time', pymongo.ASCENDING)])
        if limit:
            rows = rows.limit(limit)
        failed_job_ids = []
        for row in rows:
            try:
                job = self._reconstitute_job(row)
                jobs.append(job)
            except Exception:
                failed_job_ids.append(row['_id'])
                has_execption = True
                continue
        # Remove all the jobs we failed to restore
        if failed_job_ids:
            logging.warning('Failed to deserialize job %s, so remove it now', failed_job_ids)
            self._jobs.remove({'_id': {'$in': failed_job_ids}})
        return jobs, has_execption

    def add_job(self, job: Job) -> None:
        filter = {
            'job_id': job.job_id,
            'group': job.group,
            'subgroup': job.subgroup,
        }
        old = self._jobs.find_one(filter)
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
            self._jobs.insert_one(document)
        else:
            update = {
                'tigger': self._serializer.serialize(job.tigger),
                'call': self._serializer.serialize((job.get_func_str(), job.args)),
            }
            if job.next_time < old['next_time']:
                update['next_time'] = job.next_time
            self._jobs.update_one({'_id': old['_id']}, {
                "$set": update
            })

    def modify_job(self, job, update):
        filter = {
            'job_id': job.job_id,
            'group': job.group,
            'subgroup': job.subgroup,
        }
        self._jobs.update_one(filter, {"$set": update})

    def remove_job(self, job):
        filter = {
            'job_id': job['job_id'],
            'group': job['group'],
            'subgroup': job['subgroup'],
        }
        row = self._jobs.find_one_and_delete(filter)
        try:
            return self._reconstitute_job(row)
        except Exception:
            return None

    def record_job_exec(self, status, job, duration=0, runtimes=1, excetion=''):
        if not self._store_status:
            return
        self._schedules.insert_one({
            'job_id': job.job_id,
            'group': job.group,
            'subgroup': job.subgroup,
            'status_time': util.micro_now(),
            'status': status,
            'duration': duration,
            'runtimes': runtimes,
            'excetion': excetion
        })
        return None

    def clear_all_jobs_info(self):
        self._jobs.drop()
        self._schedules.drop()