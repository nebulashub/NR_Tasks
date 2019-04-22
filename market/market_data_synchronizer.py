import time
from abc import abstractmethod
from threading import Timer, RLock, Thread
from urllib import request

from bs4 import BeautifulSoup

from common.autowired import autowired
from common.task_log import TaskLog
from common.util.time import DateTime
from common.db import DBLock, commit, rollback


class MarketDataSynchronizer(object):
    """
    nr数据同步
    """
    _logger: TaskLog = autowired(TaskLog)
    _lock = None
    _running = False
    _timer: Timer = None

    def __init__(self):
        self._lock = RLock()

    def start(self):
        """
        server 启动时即调用开始
        :return: None
        """
        with self._lock:
            if self._running:
                return
            t = Thread(target=self._begin_sync)
            t.daemon = True
            t.start()

    # private ----------------------------------------------------------------------------------------------------------

    def _begin_sync(self):
        with self._lock:
            if self._running:
                return
            self._running = True
            try:
                if self._check_can_sync():
                    if not self._sync():
                        time.sleep(10)
                    self._running = False
                    self.start()
                else:
                    self._running = False
                    self._start_timer()
                    s = '%s start timer.' % self.__class__.__name__
                    self._logger.log(s)
            except Exception as e:
                self._logger.log_err(e)
                time.sleep(10)
                self._running = False
                self.start()

    def _sync(self) -> bool:
        try:
            str_date = DateTime(self.get_last_sync_date(), timezone_hours=0).to_str("%Y%m%d")
            market_data = self._get_market_data(str_date)
            if market_data is not None:
                with DBLock:
                    try:
                        # save to models
                        self.save_market_data(market_data)
                        # update sync date
                        self.set_last_sync_date(DateTime(timezone_hours=0).date.timestamp)
                        self._logger.log(
                            "%s market data sync success. date: %s" % (
                                self.__class__.__name__,
                                DateTime(self.get_last_sync_date(), timezone_hours=0).to_str("%Y%m%d")
                            )
                        )
                        commit()
                    except Exception as e:
                        rollback()
                        raise e
                return True
            else:
                return False
        except Exception as e:
            self._logger.log_err(e)
            return False

    # 验证是否可以继续同步
    def _check_can_sync(self) -> bool:
        return self.get_last_sync_date() < DateTime(timezone_hours=0).date.timestamp

    # 获取某一天的所有market数据
    def _get_market_data(self, date) -> list:
        url = self.url_with_date(date)
        req = request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        content = request.urlopen(req).read()
        return self._pass_market_data(content)

    def _start_timer(self):
        if self._timer is not None:
            self._timer.cancel()
        # 今天的数据，需要明天零点半后开始同步（utc+0）
        t = DateTime(timezone_hours=0).add_days(1).date.timestamp - DateTime().timestamp + 60 * 30
        self._timer = Timer(t, self.start)
        self._timer.daemon = True
        self._timer.start()

    def _pass_market_data(self, content: str) -> list:
        if content is not None:
            tb = self._get_table(content)
            if tb is not None:
                items = []
                trs = tb.find('tbody').findAll('tr')
                keys = ['date', 'opening', 'highest', 'lowest', 'closing', 'amount', 'total_circulation', 'total']
                for tr in trs:
                    item = {}
                    tds = tr.findAll('td')
                    for i in range(len(tds)):
                        key = keys[i]
                        text: str = tds[i].text
                        if key == 'date':
                            item[key] = DateTime.from_str(text, '%Y年%m月%d日', timezone_hours=0).date.timestamp
                        elif key == 'amount' or key == 'total_circulation':
                            item[key] = text.replace(',', '')
                        else:
                            item[key] = float(text)
                    item['total'] = format(self.currency_count() * item['closing'], '.0f')
                    items.append(item)
                return items
        return None

    @staticmethod
    def _get_table(content: str) -> object:
        soup = BeautifulSoup(content, 'html.parser')
        tables = soup.findAll("table")
        for t in tables:
            ths = t.findAll('th')
            for th in ths:
                if th.text == '日期':
                    return t
        return None

    # override ---------------------------------------------------------------------------------------------------------

    @abstractmethod
    def url_with_date(self, date: str) -> str:
        pass

    @abstractmethod
    def get_last_sync_date(self) -> int:
        pass

    @abstractmethod
    def set_last_sync_date(self, date: int):
        pass

    @abstractmethod
    def save_market_data(self, data: list):
        pass

    @abstractmethod
    def currency_count(self) -> int:
        pass
