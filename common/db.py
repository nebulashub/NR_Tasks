from threading import RLock

from flask import Flask
from flask_mysqldb import MySQL

import common
from common.autowired import Service, autowired


@Service()
class DataBase:
    db = None
    _app = None

    def __init__(self):
        self.init_db()

    @property
    def connection(self):
        with self._app.app_context():
            return self.db.connect

    def init_db(self):
        self.db = MySQL()
        self._app = Flask(__name__)
        self._app.config['MYSQL_HOST'] = '127.0.0.1'
        self._app.config['MYSQL_PORT'] = 3307
        self._app.config['MYSQL_USER'] = 'root'
        self._app.config['MYSQL_PASSWORD'] = 'root'
        self._app.config['MYSQL_DB'] = 'nr_db'
        self._app.config['MYSQL_CURSORCLASS'] = 'DictCursor'
        self.db.init_app(self._app)


DBLock = RLock()
DB = autowired(DataBase).connection


def execute(sql):
    try:
        with DB.cursor() as cursor:
            cursor.execute(sql, None)
    except Exception as e:
        if 'Lost connection to MySQL' in str(e):
            _re_init()
        raise e


def execute_and_fetchone(sql):
    try:
        with DB.cursor() as cursor:
            cursor.execute(sql, None)
            return cursor.fetchone()
    except Exception as e:
        if 'Lost connection to MySQL' in str(e):
            _re_init()
        raise e


def execute_and_fetchall(sql):
    try:
        with DB.cursor() as cursor:
            cursor.execute(sql, None)
            return cursor.fetchall()
    except Exception as e:
        if 'Lost connection to MySQL' in str(e):
            _re_init()
        raise e


def rollback():
    DB.rollback()


def commit():
    DB.commit()


def _re_init():
    autowired(DataBase).init_db()
    common.db.DB = autowired(DataBase).connection
