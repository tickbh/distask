from __future__ import absolute_import

from threading import Thread, Event

from distask.schedulers.base import Scheduler


class BackgroundScheduler(Scheduler):

    _thread = None
    _daemon = True

    def start(self, *args, **kwargs):
        if self._event is None or self._event.is_set():
            self._event = Event()

        Scheduler.start(self, ready=False)
        self._thread = Thread(target=self._main_loop, name='distask scheduler')
        self._thread.daemon = self._daemon
        self._thread.start()

    def shutdown(self, *args, **kwargs):
        super(BackgroundScheduler, self).shutdown(*args, **kwargs)
        self._thread.join()
        del self._thread
