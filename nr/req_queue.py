import time
from threading import Thread, RLock

import requests

from common.autowired import autowired
from common.concurrent.wait_notify import WaitNotify
from common.task_log import TaskLog
from common.util.time import DateTime
from nr.models.nr_item import NrItem

_REQ_MAX_COUNT = 1
_req = requests.Session()


class ReqQueue:
    _is_neb = None
    _begin_date: int
    _last_date: int
    _waiting_date: int
    _dates: list
    _tasks: dict
    _wn: WaitNotify
    _lock: RLock
    _logger = autowired(TaskLog)

    def __init__(self, is_neb: any, begin_date: int):
        self._is_neb = is_neb
        self._begin_date = begin_date
        self._last_date = 0
        self._waiting_date = 0
        self._dates = []
        self._tasks = {}
        self._wn = WaitNotify()
        self._lock = RLock()

    def start(self):
        with self._lock:
            now = DateTime(timezone_hours=0).date.timestamp
            dt = self._begin_date
            if self._last_date != 0:
                dt = self._last_date
            while dt < now:
                self._dates.append(dt)
                dt = DateTime(dt, timezone_hours=0).date.add_days(1).timestamp
                self._last_date = dt
            self._check_and_req()

    def get(self, date):
        with self._lock:
            data = self._get_data(date)
            if data is None:
                self._waiting_date = date
                self._wn.reset()
        if data is None:
            self._wn.wait()
            data = self._get_data(date)
        self._waiting_date = 0
        return data

    def remove(self, date):
        with self._lock:
            self._tasks.pop(date)
            self._check_and_req()

    def _get_data(self, date):
        with self._lock:
            if date in self._tasks.keys() and self._tasks[date].data is not None:
                return self._tasks[date].data
            return None

    def _did_data_loaded(self, date: int):
        with self._lock:
            if date == self._waiting_date:
                self._wn.notify()

    def _check_and_req(self):
        with self._lock:
            c = len(self._tasks)
            if c >= _REQ_MAX_COUNT:
                return
            n = len(self._dates)
            if n == 0:
                return
            m = min(_REQ_MAX_COUNT - c, n)
            r = []
            for i in range(m):
                dt = self._dates[i]
                task = _ReqTask(dt, self._is_neb, self._did_data_loaded)
                r.append(dt)
                self._tasks[dt] = task
                task.start()
            for d in r:
                self._dates.remove(d)


class _ReqTask:
    _date: int
    _is_neb: bool
    _logger: TaskLog = autowired(TaskLog)

    did_load_data = None
    data: list

    def __init__(self, date: int, is_neb: bool, did_load_data: any):
        self._date = date
        self._is_neb = is_neb
        self.did_load_data = did_load_data
        self.data = None

    def start(self):
        t = Thread(target=self._begin)
        t.daemon = True
        t.start()

    def _begin(self):
        str_date = DateTime(self._date, timezone_hours=0).to_str('%Y%m%d')
        ok, data = _Request(str_date, self._is_neb).get_result()
        if ok and len(data) > 0:
            self.data = data
            self.did_load_data(self._date)
        else:
            time.sleep(10)
            self.start()


class _Request:
    _id: str
    _date: str
    _is_neb: bool
    _data: list
    _success: bool
    _logger = autowired(TaskLog)

    def __init__(self, date: str, is_neb: bool):
        self._id = None
        self._date = date
        self._is_neb = is_neb
        self._data = list()
        self._success = True

    def get_result(self) -> (bool, list):
        try:
            self._req()
            return self._success, self._data
        except Exception as e:
            self._logger.log_err(e)
            self._success = False
            return self._success, None

    def _req(self):
        db = 'nebulas'
        if not self._is_neb:
            db = 'eth'
        while True:
            if self._id is None:
                url = 'http://111.203.228.11:9973/nr?db=' + db + '&batch_size=1000&date=' + self._date
                self._logger.log('url: ' + url)
            else:
                url = 'http://111.203.228.11:9973/cursor?db=' + db + '&id=' + self._id
            resp = _req.get(url)
            if 300 > resp.status_code >= 200:
                r: dict = resp.json()
                for i in r['result']:
                    self._data.append(NrItem(i))
                if 'id' in r.keys():
                    self._id = r['id']
                if not r['has_more']:
                    break
            else:
                if resp.status_code == 503:
                    print('503')
                    TaskLog.log_rewrite('503.log', url)
                    time.sleep(0.1)
                    continue
                else:
                    self._logger.log_err(resp)
                    self._success = False
                    break
