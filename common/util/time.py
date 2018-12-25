import calendar
from datetime import datetime, timedelta, timezone
import time


class DateTime(object):
    _dt: datetime = None

    def __init__(self, timestamp=None, timezone_hours=None):
        if timestamp is None:
            self._dt = datetime.now()
        else:
            self._dt = datetime.fromtimestamp(timestamp)
        if timezone_hours is not None:
            self._tz = timezone_hours
        else:
            self._tz = self._get_tz()

    @staticmethod
    def from_str(str_datetime, fmt, timezone_hours=None):
        dt = datetime.strptime(str_datetime, fmt)
        return DateTime(timestamp=int(time.mktime(dt.timetuple())) + (DateTime._get_tz() * 60 * 60),
                        timezone_hours=timezone_hours)

    @property
    def timestamp(self):
        return int(time.mktime(self._dt.timetuple()))

    @property
    def date(self):
        dt = self._dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return DateTime(int(time.mktime(dt.timetuple())) + (DateTime._get_tz() * 60 * 60), timezone_hours=self._tz)

    def to_str(self, fmt="%Y-%m-%d %H:%M:%S.%f"):
        ts = int(time.mktime(self._dt.timetuple())) + (self._tz - self._get_tz()) * 60 * 60
        return datetime.fromtimestamp(ts).strftime(fmt)

    def add_days(self, days):
        self._dt = self._dt + timedelta(days=days)
        return self

    def add_months(self, months):
        month = self._dt.month - 1 + months
        year = self._dt.year + month / 12
        month = month % 12 + 1
        day = min(self._dt.day, calendar.monthrange(year, month)[1])
        self._dt = self._dt.replace(year=year, month=month, day=day)
        return self

    def add_years(self, years):
        year = self._dt.year + years
        self._dt = self._dt.replace(year=year)
        return self

    @staticmethod
    def _get_tz():
        s = time.strftime('%z', time.localtime()).rstrip('0')
        if s == '+' or s == '-':
            s = '0'
        return int(s)
