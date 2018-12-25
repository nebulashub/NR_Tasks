import time
from threading import Thread

from common.concurrent.wait_notify import WaitNotify
from common.signal_log import start_signal_log
from common.autowired import autowired
from common.task_log import TaskLog
from market.eth_market_data_synchronizer import EthMarketDataSynchronizer
from market.neb_market_data_synchronizer import NebMarketDataSynchronizer
from nr.eth_nr_data_synchronizer import EthNrDataSynchronizer
from nr.neb_nr_data_synchronizer import NebNrDataSynchronizer

_logger: TaskLog = autowired(TaskLog)
_wn: WaitNotify = WaitNotify()


def start_tasks():
    _neb_nr: NebNrDataSynchronizer = autowired(NebNrDataSynchronizer)
    _eth_nr: EthNrDataSynchronizer = autowired(EthNrDataSynchronizer)
    _neb_market: NebMarketDataSynchronizer = autowired(NebMarketDataSynchronizer)
    _eth_market: EthMarketDataSynchronizer = autowired(EthMarketDataSynchronizer)

    _logger.log("task launch.")
    _neb_nr.start()
    _eth_nr.start()
    _neb_market.start()
    _eth_market.start()


def _h():
    while True:
        TaskLog.log_heartbeat()
        time.sleep(10)


def start_heartbeat():
    t = Thread(target=_h)
    t.daemon = False
    t.start()


if __name__ == '__main__':
    start_signal_log()
    start_tasks()
    start_heartbeat()
    _wn.wait()
