import os
import sys

from common.autowired import Service
from common.util.time import DateTime

os.chdir(sys.path[0])
root_dir = os.path.abspath('./')


@Service()
class TaskLog:
    _f = open(os.path.join(root_dir, 'task.log'), 'a')
    _e = open(os.path.join(root_dir, 'task_error.log'), 'a')

    def log(self, log):
        print('task log:', log)
        self._f.write('[' + DateTime().to_str() + '] ' + str(log) + '\n')
        self._f.flush()

    def log_err(self, log):
        print('task error log:', log)
        self._e.write('[' + DateTime().to_str() + '] ' + str(log) + '\n')
        self._e.flush()

    @staticmethod
    def log_rewrite(f, log):
        with open(os.path.join(root_dir, f), 'w') as h:
            h.write('[' + DateTime().to_str() + '] ' + str(log) + '\n')
            h.flush()

    @staticmethod
    def log_heartbeat():
        TaskLog.log_rewrite('heartbeat.log', '')
