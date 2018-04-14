import os
import time

import sqlite3

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    from threading import get_ident
except ImportError:
    from thread import get_ident

from pandaharvester.harvestercore.plugin_base import PluginBase


class SqliteQueue(PluginBase):

    # template of SQL commands
    _create_sql = (
            'CREATE TABLE IF NOT EXISTS queue_table '
            '('
            '  id INTEGER PRIMARY KEY AUTOINCREMENT,'
            '  item BLOB'
            ')'
            )
    _count_sql = 'SELECT COUNT(*) FROM queue_table'
    _iterate_sql = 'SELECT id, item FROM queue_table'
    _write_lock_sql = 'BEGIN IMMEDIATE'
    _rpush_sql = 'INSERT INTO queue_table (item) VALUES (?)'
    _lpop_get_sql = (
            'SELECT id, item FROM queue_table '
            'ORDER BY id LIMIT 1'
            )
    _rpop_get_sql = (
            'SELECT id, item FROM queue_table '
            'ORDER BY id DESC LIMIT 1'
            )
    _pop_del_sql = 'DELETE FROM queue_table WHERE id = ?'
    _peek_sql = (
            'SELECT item FROM queue_table '
            'ORDER BY id LIMIT 1'
            )

    # constructor
    def __init__(self):
        self.db_path = os.path.abspath(self.database_filename)
        self._connection_cache = {}
        with self._get_conn() as conn:
            conn.execute(self._create_sql)

    def __len__(self):
        with self._get_conn() as conn:
            size = conn.execute(self._count_sql).next()[0]
        return size

    def __iter__(self):
        with self._get_conn() as conn:
            for id, obj_buf in conn.execute(self._iterate_sql):
                yield pickle.loads(str(obj_buf))

    def _get_conn(self):
        id = get_ident()
        if id not in self._connection_cache:
            self._connection_cache[id] = sqlite3.Connection(self.db_path, timeout=60)
        return self._connection_cache[id]

    def _push(self, obj, push_sql):
        obj_buf = memoryview(pickle.dumps(obj, -1))
        with self._get_conn() as conn:
            conn.execute(push_sql, (obj_buf,))

    def _pop(self, get_sql, sleep_wait=True):
        keep_pooling = True
        wait = 0.1
        max_wait = 2
        tries = 0
        with self._get_conn() as conn:
            id = None
            while keep_pooling:
                conn.execute(self._write_lock_sql)
                cursor = conn.execute(get_sql)
                try:
                    id, obj_buf = cursor.next()
                    keep_pooling = False
                except StopIteration:
                    # unlock the database
                    conn.commit()
                    if not sleep_wait:
                        keep_pooling = False
                        continue
                    tries += 1
                    time.sleep(wait)
                    wait = min(max_wait, tries/10 + wait)
            if id:
                conn.execute(self._pop_del_sql, (id,))
                conn.commit()
                return pickle.loads(str(obj_buf))
        return None

    def _peek(self):
        with self._get_conn() as conn:
            cursor = conn.execute(self._peek_sql)
            try:
                return pickle.loads(str(cursor.next()[0]))
            except StopIteration:
                return None

    # number of objects in queue
    def size(self):
        return len(self)

    # enqueue
    def put(self, obj):
        self._push(obj, push_sql=_rpush_sql)

    # dequeue
    def get(self):
        return self._pop(get_sql=_lpop_get_sql)

    # dequeue the last
    def getlast(self):
        return self._pop(get_sql=_rpop_get_sql)
