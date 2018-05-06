import os
import time
import re

import sqlite3

try:
    from threading import get_ident
except ImportError:
    from thread import get_ident

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config

try:
    memoryviewOrBuffer = buffer
except NameError:
    memoryviewOrBuffer = memoryview


class SqliteFifo(PluginBase):

    # template of SQL commands
    _create_sql = (
            'CREATE TABLE IF NOT EXISTS queue_table '
            '('
            '  id INTEGER PRIMARY KEY,'
            '  item BLOB,'
            '  score REAL'
            ')'
            )
    _create_index_sql = (
            'CREATE INDEX IF NOT EXISTS score_index ON queue_table '
            '(score)'
            )
    _count_sql = 'SELECT COUNT(id) FROM queue_table'
    _iterate_sql = 'SELECT id, item, score FROM queue_table'
    _write_lock_sql = 'BEGIN IMMEDIATE'
    _exclusive_lock_sql = 'BEGIN EXCLUSIVE'
    _push_sql = 'INSERT INTO queue_table (item,score) VALUES (?,?)'
    _push_with_id_sql = 'INSERT INTO queue_table (id,item,score) VALUES (?,?,?)'
    _lpop_get_sql = (
            'SELECT id, item, score FROM queue_table '
            'ORDER BY score LIMIT 1'
            )
    _rpop_get_sql = (
            'SELECT id, item, score FROM queue_table '
            'ORDER BY score DESC LIMIT 1'
            )
    _pop_del_sql = 'DELETE FROM queue_table WHERE id = ?'
    _clear_delete_table_sql = 'DELETE FROM queue_table'
    _clear_zero_id_sql = 'DELETE FROM sqlite_sequence WHERE name = "queue_table"'
    _peek_sql = (
            'SELECT item, score FROM queue_table '
            'ORDER BY score LIMIT 1'
            )
    _peek_id_sql = (
            'SELECT id FROM queue_table '
            'ORDER BY score LIMIT 1'
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
            conn.execute(self._exclusive_lock_sql)
            conn.execute(self._create_sql)
            conn.execute(self._create_index_sql)
            conn.commit()

    def __len__(self):
        with self._get_conn() as conn:
            size = next(conn.execute(self._count_sql))[0]
        return size

    def __iter__(self):
        with self._get_conn() as conn:
            for id, obj_buf, score in conn.execute(self._iterate_sql):
                yield bytes(obj_buf)

    def _get_conn(self):
        id = get_ident()
        if id not in self._connection_cache:
            self._connection_cache[id] = sqlite3.Connection(self.db_path, timeout=60)
        return self._connection_cache[id]

    def _push(self, obj, score, push_sql):
        obj_buf = memoryviewOrBuffer(obj)
        with self._get_conn() as conn:
            conn.execute(push_sql, (obj_buf, score))

    # def _push_right(self, obj, score):
    #     obj_buf = memoryviewOrBuffer(obj)
    #     with self._get_conn() as conn:
    #         conn.execute(self._exclusive_lock_sql)
    #         cursor = conn.execute(self._peek_id_sql)
    #         first_id = None
    #         try:
    #             first_id, = next(cursor)
    #         except StopIteration:
    #             pass
    #         finally:
    #             id = (first_id - 1) if first_id is not None else 0
    #             conn.execute(self._push_with_id_sql, (id, obj_buf, score))
    #         conn.commit()

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
                    id, obj_buf, score = next(cursor)
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
            if id is not None:
                conn.execute(self._pop_del_sql, (id,))
                conn.commit()
                return bytes(obj_buf)
        return None

    # number of objects in queue
    def size(self):
        return len(self)

    # enqueue with priority score
    def put(self, obj, score):
        self._push(obj, score, push_sql=self._push_sql)

    # dequeue the first object
    def get(self, timeout=None):
        return self._pop(get_sql=self._lpop_get_sql, timeout=None)

    # dequeue the last object
    def getlast(self, timeout=None):
        return self._pop(get_sql=self._rpop_get_sql, timeout=None)

    # get tuple of (obj, score) of the first object without dequeuing it
    def peek(self):
        with self._get_conn() as conn:
            cursor = conn.execute(self._peek_sql)
            try:
                obj_buf, score = next(cursor)
                return bytes(obj_buf), score
            except StopIteration:
                return None, None

    # drop all objects in queue and index and reset primary key auto_increment
    def clear(self):
        with self._get_conn() as conn:
            conn.execute(self._exclusive_lock_sql)
            conn.execute(self._clear_delete_table_sql)
            try:
                conn.execute(self._clear_zero_id_sql)
            except sqlite3.OperationalError:
                pass
            conn.commit()
