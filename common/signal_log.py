import signal

from common.autowired import autowired
from common.task_log import TaskLog

logger = autowired(TaskLog)


def on_signal_int():
    logger.log_err('signal int')


def on_signal_term():
    logger.log_err('signal term')


def start_signal_log():
    signal.signal(signal.SIGINT, on_signal_int)
    signal.signal(signal.SIGTERM, on_signal_term)
