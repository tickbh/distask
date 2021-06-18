
from distask import create_scheduler

def create_base_scheduler(db="mongo", limit=10, maxwait=10):
    client_data = {
        "t": "mongo",
        "client_args": {
            "host": "mongodb://admin:123456@192.168.99.27:27017"
        },
        "store_args": {
            "database": 'testcase_distask', 
            "schedules": 'schedules', 
            "jobs": 'jobs',
        }
    }
    if db == "redis":
        client_data = {
            't': 'redis',
            "host":'127.0.0.1', 
            'port':6379,
            'db':15, 
        }
    
    connection_details=[
        {'host': '127.0.0.1', 'port': 6379, 'db': 15},
    ]
    lock_data = {
        "t": "rllock",
        "reentrant":True, 
        "connection_details":connection_details, 
        "ttl":10_000
    }
    scheduler = create_scheduler(client_data, lock_data, serialize="pickle", limit=limit, maxwait=maxwait)
    scheduler.clear_all_jobs()
    return scheduler