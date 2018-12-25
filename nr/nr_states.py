from threading import RLock

from common.autowired import Component
from common.cache import NRLocalMemCache
from common.util.json import JsonUtil
from common.util.time import DateTime
from common.db import DBLock, execute, commit, execute_and_fetchall, execute_and_fetchone


class DbStates(object):
    """
    需要持久化的状态类型数据管理类(Db)
    """
    _lock = RLock()

    @classmethod
    def get(cls, key, default_value=None):
        with cls._lock:
            with DBLock:
                sql = 'SELECT `value` FROM state WHERE `key` = \'%s\';' % key
                r = execute_and_fetchall(sql)
                if len(r) > 0:
                    return JsonUtil.deserialize(r[0]['value'])
                else:
                    return default_value

    @classmethod
    def set(cls, key, value):
        value = JsonUtil.serialize(value)
        with cls._lock:
            if cls._has_key(key):
                sql = 'UPDATE state SET `value` = \'%s\' WHERE `key` = \'%s\';' % (value, key)
            else:
                sql = 'INSERT INTO state (`key`, `value`) VALUES (\'%s\', \'%s\');' % (key, value)
            with DBLock:
                execute(sql)
                commit()

    @classmethod
    def _has_key(cls, key):
        sql = 'SELECT COUNT(*) as c FROM state WHERE `key` = \'%s\';' % key
        with DBLock:
            r = execute_and_fetchone(sql)
            return r['c'] > 0


@Component()
class NRState(object):
    """
    需要持久化的状态类型数据管理类(Cache & Db)
    """
    _neb_key_last_sync_date = "neb_last_sync_date"
    _neb_key_market_last_sync_date = "neb_market_last_sync_date"
    _neb_begin_date = DateTime.from_str("20180508", "%Y%m%d", timezone_hours=0).timestamp
    # _neb_begin_date = DateTime.from_str("20181127", "%Y%m%d", timezone_hours=0).timestamp

    _eth_key_last_sync_date = "eth_last_sync_date"
    _eth_key_market_last_sync_date = "eth_market_last_sync_date"
    _eth_begin_date = DateTime.from_str("20170101", "%Y%m%d", timezone_hours=0).timestamp

    # neb nr date ------------------------------------------------------------------------------------------------------
    @property
    def neb_last_sync_date(self):
        return self.get_last_date(self._neb_key_last_sync_date)

    @neb_last_sync_date.setter
    def neb_last_sync_date(self, value):
        self.set_last_date(self._neb_key_last_sync_date, value)

    @property
    def neb_current_sync_date(self):
        return self.get_current_date(self._neb_begin_date, self.neb_last_sync_date)

    # neb market date --------------------------------------------------------------------------------------------------
    @property
    def neb_market_last_sync_date(self):
        d = self.get_last_date(self._neb_key_market_last_sync_date)
        if d == 0:
            d = self._neb_begin_date
        return d

    @neb_market_last_sync_date.setter
    def neb_market_last_sync_date(self, value):
        self.set_last_date(self._neb_key_market_last_sync_date, value)

    # eth nr date ------------------------------------------------------------------------------------------------------
    @property
    def eth_last_sync_date(self):
        return self.get_last_date(self._eth_key_last_sync_date)

    @eth_last_sync_date.setter
    def eth_last_sync_date(self, value):
        self.set_last_date(self._eth_key_last_sync_date, value)

    @property
    def eth_current_sync_date(self):
        return self.get_current_date(self._eth_begin_date, self.eth_last_sync_date)

    # eth market date --------------------------------------------------------------------------------------------------
    @property
    def eth_market_last_sync_date(self):
        d = self.get_last_date(self._eth_key_market_last_sync_date)
        if d == 0:
            d = self._eth_begin_date
        return d

    @eth_market_last_sync_date.setter
    def eth_market_last_sync_date(self, value):
        self.set_last_date(self._eth_key_market_last_sync_date, value)

    # private ----------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_last_date(key):
        ts = NRLocalMemCache.get(key)
        if ts is None:
            ts = DbStates.get(key)
        if ts is None:
            ts = 0
        return ts

    @staticmethod
    def set_last_date(key, value):
        ts = DateTime(timestamp=value, timezone_hours=0).date.timestamp
        DbStates.set(key, ts)
        NRLocalMemCache.set(key, ts)

    @staticmethod
    def get_current_date(begin_date, last_date):
        if last_date == 0:
            return begin_date
        else:
            return DateTime(timestamp=last_date, timezone_hours=0).add_days(1).timestamp
