import os
import time
import re

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
from pandaharvester.harvesterconfig import harvester_config


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
    _exclusive_lock_sql = 'BEGIN EXCLUSIVE'
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
    _clear_delete_table_sql = 'DELETE FROM queue_table'
    _clear_zero_id_sql = 'DELETE FROM sqlite_sequence WHERE name = "queue_table"'
    _peek_sql = (
            'SELECT item FROM queue_table '
            'ORDER BY id LIMIT 1'
            )

    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        if hasattr(self, 'database_filename'):
            _db_filename = self.database_filename
        else:
            _db_filename = harvester_config.fifo.database_filename
        self.db_path = os.path.abspath(re.sub('\$\(AGENT\)', self.agentName, _db_filename))
        self._connection_cache = {}
        with self._get_conn() as conn:
            conn.execute(self._create_sql)

    def __len__(self):
        with self._get_conn() as conn:
            size = next(conn.execute(self._count_sql))[0]
        return size

    def __iter__(self):
        with self._get_conn() as conn:
            for id, obj_buf in conn.execute(self._iterate_sql):
                yield pickle.loads(bytes(obj_buf))

    def _get_conn(self):
        id = get_ident()
        if id not in self._connection_cache:
            self._connection_cache[id] = sqlite3.Connection(self.db_path, timeout=60)
        return self._connection_cache[id]

    def _push(self, obj, push_sql):
        obj_buf = memoryview(pickle.dumps(obj, -1))
        with self._get_conn() as conn:
            conn.execute(push_sql, (obj_buf,))

    def _pop(self, get_sql, timeout=None):
        keep_polling = True
        wait = 0.1
        max_wait = 2
        tries = 0
        last_attempt_timestamp = time.time()
        with self._get_conn() as conn:
            id = None
            while keep_polling:
                conn.execute(self._write_lock_sql)
                cursor = conn.execute(get_sql)
                try:
                    id, obj_buf = next(cursor)
                    keep_polling = False
                except StopIteration:
                    # unlock the database
                    conn.commit()
                    now_timestamp = time.time()
                    if (now_timestamp - last_attempt_timestamp) >= timeout:
                        keep_polling = False
                        continue
                    tries += 1
                    time.sleep(wait)
                    wait = min(max_wait, tries/10.0 + wait)
            if id:
                conn.execute(self._pop_del_sql, (id,))
                conn.commit()
                return pickle.loads(bytes(obj_buf))
        return None

    # number of objects in queue
    def size(self):
        return len(self)

    # enqueue
    def put(self, obj):
        self._push(obj, push_sql=self._rpush_sql)

    # dequeue the first object
    def get(self, timeout=None):
        return self._pop(get_sql=self._lpop_get_sql, timeout=None)

    # dequeue the last object
    def getlast(self, timeout=None):
        return self._pop(get_sql=self._rpop_get_sql, timeout=None)

    # get the first object without dequeuing it
    def peek(self):
        with self._get_conn() as conn:
            cursor = conn.execute(self._peek_sql)
            try:
                return pickle.loads(bytes(next(cursor)[0]))
            except StopIteration:
                return None

    # drop all objects in queue and reset primary key auto_increment
    def clear(self):
        with self._get_conn() as conn:
            conn.execute(self._exclusive_lock_sql)
            conn.execute(self._clear_delete_table_sql)
            try:
                conn.execute(self._clear_zero_id_sql)
            except sqlite3.OperationalError:
                pass
            conn.commit()
