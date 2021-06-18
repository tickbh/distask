
 
from distask.locks.base import BaseLock
import logging, os, time
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock
 
class ZKLock(BaseLock):
    def __init__(self, hosts=None, name="distask_zklock", timeout=10):
        """
        :param hosts: zookeeper主机地址
        :param name: 分布式锁名称
        :param logger: 日志对象
        :param timeout: 连接超时
        """
        self._client = None
        self._lock = None
 
        # 创建客户端对象并初始化连接
        try:
            self._client = KazooClient(hosts=hosts, timeout=timeout)
            self._client.start(timeout=timeout)
        except Exception as e:
            logging.error('Create KazooClient Failed! Exception:{}'.format(e))
            raise
 
        # 创建Lock对象
        try:
            lock_path = os.path.join("/", "locks", name)
            self._lock = Lock(self._client, lock_path)
        except Exception as e:
            logging.error('Create Lock Failed! Exception: %s'.format(e))
            raise
 
    def __del__(self):
        """
        :return:
        """
        self.unlock()
        if self._client:
            self._client.stop()
            self._client = None
 
    def lock(self, blocking=True, timeout=None):
        """
        """
        if self._lock is None:
            return False
 
        try:
            return self._lock.acquire(blocking=blocking, timeout=timeout)
        except Exception as e:
            logging.error('Acquire lock failed! Exception: %s'.format(e))
            return False
 
    def unlock(self):
        """
        """
        if self._lock is not None:
            self._lock.release()
            logging.info('Release Lock')
