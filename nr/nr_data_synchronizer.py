import time
import zlib
from abc import abstractmethod
from threading import Timer, RLock, Thread

from common.autowired import autowired
from common.task_log import TaskLog
from common.util.time import DateTime
from common.db import DBLock, execute, commit, rollback, execute_and_fetchall, execute_and_fetchone
from nr.models.address_item import AddressItem
from nr.models.nr_item import NrItem
from nr.req_queue import ReqQueue
from proto.nr_data_pb2 import Item, Data


class NrDataSynchronizer(object):
    """
    nr数据同步
    """
    _logger: TaskLog = autowired(TaskLog)
    _lock: RLock
    _running = False
    _timer: Timer = None
    _req_queue: ReqQueue

    _SEVEN_DAY_SECONDS = 7 * 24 * 60 * 60

    def __init__(self):
        self._lock = RLock()
        self._req_queue = ReqQueue(self.is_neb(), self.get_current_sync_date())

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
            self._req_queue.start()

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
                    self._logger.log('%s start timer.' % self.__class__.__name__)
            except Exception as e:
                self._logger.log_err(e)
                time.sleep(10)
                self._running = False
                self.start()

    def _sync(self) -> bool:
        try:
            # str_date = DateTime(self.get_current_sync_date(), timezone_hours=0).to_str("%Y%m%d")
            dt = self.get_current_sync_date()
            date_table, _, _ = self.db_context()
            if self._exists(date_table, dt):
                self.set_last_sync_date(dt)
                self._logger.log_err('duplicate date: %s' % DateTime(dt, timezone_hours=0).to_str('%Y-%m-%d'))
                return True
            nr_data = self._req_queue.get(dt)
            # nr_data = self._get_daily_all_nr(str_date)
            if nr_data is not None:
                with DBLock:
                    try:
                        # save to models
                        self._sort_nr_data(nr_data)
                        self._save_nr_data(nr_data)
                        # update sync date
                        self.set_last_sync_date(dt)
                        self._logger.log(
                            "%s nr_data sync success. date: %s" % (
                                self.__class__.__name__,
                                DateTime(self.get_last_sync_date(), timezone_hours=0).to_str("%Y%m%d")
                            )
                        )
                        commit()
                        self._req_queue.remove(dt)
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
        return self.get_current_sync_date() < DateTime(timezone_hours=0).date.timestamp

    @staticmethod
    def _sort_key(nr: dict) -> float:
        return float(nr.score)

    @staticmethod
    def _sort_nr_data(nr_data: list):
        nr_data.sort(key=NrDataSynchronizer._sort_key, reverse=True)
        for i in range(len(nr_data)):
            nr_data[i].order = i + 1

    def _start_timer(self):
        if self._timer is not None:
            self._timer.cancel()
        # 今天的数据，需要明天零点半后开始同步（utc+0）
        t = DateTime(timezone_hours=0).add_days(1).date.timestamp - DateTime().timestamp + 60 * 30
        self._timer = Timer(t, self.start)
        self._timer.daemon = True
        self._timer.start()

    # tools ------------------------------------------------------------------------------------------------------------

    @staticmethod
    def total_nr(nr_data: list) -> str:
        t: float = 0.0
        for nr in nr_data:
            t += float(nr.score)
        return format(t, ".2f")

    @staticmethod
    def add_nr_to_pb_data(pb_data, nr: NrItem):
        item = pb_data.items.add()
        NrDataSynchronizer.init_pb_item(item, nr)

    @staticmethod
    def new_pb_item(nr: NrItem) -> any:
        item = Item()
        NrDataSynchronizer.init_pb_item(item, nr)
        return item

    @staticmethod
    def init_pb_item(item, nr: NrItem):
        item.address = nr.address
        item.in_outs = nr.in_outs
        item.out_val = nr.out_val
        item.in_val = nr.in_val
        item.degrees = nr.degrees
        item.out_degree = nr.out_degree
        item.in_degree = nr.in_degree
        item.weight = nr.weight
        item.median = nr.median
        item.score = nr.score
        item.date = nr.date
        item.order = nr.order

    @staticmethod
    def serialize_nr_data(nr_data: list) -> bytes:
        data = Data()
        for nr in nr_data:
            NrDataSynchronizer.add_nr_to_pb_data(data, nr)
        return zlib.compress(data.SerializeToString())

    def get_valid_dates(self, dates: str):
        r = []
        if dates is None or len(dates) == 0:
            return r
        now = DateTime().timestamp
        dts = dates.split(',')
        for d in dts:
            if now - int(d) <= self._SEVEN_DAY_SECONDS:
                r.append(d)
        return r

    # db ---------------------------------------------------------------------------------------------------------------

    def _save_nr_data(self, nr_data: list):
        date_table, total_table, address_table = self.db_context()
        self._save_date_data(date_table, self.get_current_sync_date(), nr_data)
        self._update_address_table(address_table, nr_data)
        self._save_total_nr(total_table, nr_data)

    def _save_date_data(self, db_table: str, date: int, nr_data: list):
        count = len(nr_data)
        s = self.serialize_nr_data(nr_data)
        loc = 0
        first = True
        while True:
            c = 1024 * 1024 * 1
            if c > len(s) - loc:
                c = len(s) - loc
            if c <= 0:
                break
            t = s[loc: loc + c]
            loc = loc + c
            h = ''.join(['%02x' % b for b in t])
            if first:
                sql = 'INSERT INTO %s (`date`, `data`, `count`) VALUES (\'%s\', X\'%s\', \'%s\');' % \
                      (db_table, str(date), h, str(count))
                first = False
            else:
                sql = 'UPDATE %s SET `data` = concat(`data`, X\'%s\') WHERE `date`=\'%s\';' \
                      % (db_table, h, str(date))
            execute(sql)

    def _update_address_table(self, db_table, nr_data: list):
        addresses = self._addresses_from_db(db_table)
        n = 0
        sql = ''
        for nr in nr_data:
            sql += self._add_to_address(db_table, addresses, nr)
            n += 1
            if n >= 1000:
                execute(sql)
                n = 0
                sql = ''
        if n > 0:
            execute(sql)

    def _save_total_nr(self, total_table, nr_data):
        sql = 'INSERT INTO %s (`date`, `nr_value`) VALUES (%s, \'%s\')' % \
              (total_table, str(self.get_current_sync_date()), self.total_nr(nr_data))
        execute(sql)

    def _add_to_address(self, db_table, addresses: dict, nr: NrItem) -> str:
        a = nr.address
        item = self.new_pb_item(nr)
        now = DateTime().timestamp
        c = self.get_current_sync_date()
        if a in addresses.keys():
            count = addresses[a].count + 1
            total = addresses[a].total_nr
            if total is None:
                total = 0
            else:
                total = float(total)
            total += float(item.score)

            i_bytes = b'|' + item.SerializeToString()
            i_hex = ''.join(['%02x' % b for b in i_bytes])
            dates = self.get_valid_dates(addresses[a].dates)
            if float(item.score) >= 0.1 and now - c <= self._SEVEN_DAY_SECONDS:
                dates.append(str(c))
            n = len(dates)
            str_dts = ','.join(dates)
            return 'UPDATE %s SET last_above_0_dates=\'%s\', last_above_0_num=\'%s\', `count`=\'%s\', total_nr=\'%s\', `data`=concat(`data`, X\'%s\') WHERE `address`=\'%s\';' % \
                   (db_table, str_dts, str(n), str(count), str(total), i_hex, a)
        else:
            count = 1
            total = float(item.score)
            i_bytes = item.SerializeToString()
            i_hex = ''.join(['%02x' % b for b in i_bytes])
            dates = []
            if float(item.score) >= 0.1 and now - c <= self._SEVEN_DAY_SECONDS:
                dates.append(str(c))
            n = len(dates)
            str_dts = ','.join(dates)
            return 'INSERT INTO %s (`address`, `last_above_0_dates`, `last_above_0_num`, `count`, total_nr, `data`) VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', X\'%s\');' % \
                   (db_table, a, str_dts, str(n), str(count), str(total), i_hex)

    @staticmethod
    def _addresses_from_db(db_table: str) -> dict:
        rows = execute_and_fetchall('SELECT address, last_above_0_dates, last_above_0_num, `count`, total_nr FROM %s;' %
                                    db_table)
        result = {}
        for r in rows:
            result[r['address']] = AddressItem(r['last_above_0_dates'], r['count'], r['total_nr'])
        return result

    @staticmethod
    def _exists(db_table: str, date: int) -> bool:
        with DBLock:
            sql = 'SELECT COUNT(*) as c FROM %s WHERE `date` = %s;' % (db_table, str(date))
            row = execute_and_fetchone(sql)
            return row['c'] > 0

    # override ---------------------------------------------------------------------------------------------------------

    @abstractmethod
    def is_neb(self) -> bool:
        pass

    @abstractmethod
    def get_last_sync_date(self) -> int:
        pass

    @abstractmethod
    def set_last_sync_date(self, date: int):
        pass

    @abstractmethod
    def get_current_sync_date(self) -> int:
        pass

    @abstractmethod
    def db_context(self):
        pass
