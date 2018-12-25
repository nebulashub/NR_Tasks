from threading import Event, Lock


class WaitNotify(object):

    _event: Event = None
    _lock: Lock = None
    _lock_wait: Lock = None

    def __init__(self):
        self._lock = Lock()
        self._event = Event()
        self._lock_wait = Lock()

    def wait(self):
        with self._lock_wait:
            with self._lock:
                if self._event.is_set():
                    return
            self._event.wait()

    def notify(self):
        with self._lock:
            if not self._event.is_set():
                self._event.set()

    def reset(self):
        if self._event.is_set():
            self._event.clear()
