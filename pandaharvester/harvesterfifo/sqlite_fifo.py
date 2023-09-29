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
    _create_sql = "CREATE TABLE IF NOT EXISTS queue_table " "(" "  id INTEGER PRIMARY KEY," "  item BLOB," "  score REAL," "  temporary INTEGER DEFAULT 0 " ")"
    _create_index_sql = "CREATE INDEX IF NOT EXISTS score_index ON queue_table " "(score)"
    _count_sql = "SELECT COUNT(id) FROM queue_table"
    _iterate_sql = "SELECT id, item, score FROM queue_table"
    _write_lock_sql = "BEGIN IMMEDIATE"
    _exclusive_lock_sql = "BEGIN EXCLUSIVE"
    _push_sql = "INSERT INTO queue_table (item,score) VALUES (?,?)"
    _push_by_id_sql = "INSERT OR IGNORE INTO queue_table (id,item,score) VALUES (?,?,?)"
    _lpop_get_sql_template = "SELECT {columns} FROM queue_table " "WHERE temporary = 0 " "ORDER BY score LIMIT 1"
    _rpop_get_sql_template = "SELECT {columns} FROM queue_table " "WHERE temporary = 0 " "ORDER BY score DESC LIMIT 1"
    _get_by_id_sql_template = "SELECT {columns} FROM queue_table " "WHERE id = ? " "AND temporary = {temp}"
    _get_many_template = (
        "SELECT id, item, score FROM queue_table " "WHERE " "{temporary_str} " "{minscore_str} " "{maxscore_str} " "ORDER BY score {rank} " "{count_str} "
    )
    _pop_del_sql = "DELETE FROM queue_table WHERE id = ?"
    _move_to_temp_sql = "UPDATE queue_table SET temporary = 1 WHERE id = ?"
    _move_many_to_temp_sql_template = "UPDATE queue_table SET temporary = 1 WHERE id in ({0})"
    _del_sql_template = "DELETE FROM queue_table WHERE id in ({0})"
    _clear_delete_table_sql = "DELETE FROM queue_table"
    _clear_drop_table_sql = "DROP TABLE IF EXISTS queue_table"
    _clear_zero_id_sql = 'DELETE FROM sqlite_sequence WHERE name = "queue_table"'
    _peek_sql = "SELECT id, item, score FROM queue_table " "WHERE temporary = 0 " "ORDER BY score LIMIT 1"
    _restore_sql = "UPDATE queue_table SET temporary = 0 WHERE temporary != 0"
    _restore_sql_template = "UPDATE queue_table SET temporary = 0 " "WHERE temporary != 0 AND id in ({0})"

    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        if hasattr(self, "database_filename"):
            _db_filename = self.database_filename
        else:
            _db_filename = harvester_config.fifo.database_filename
        _db_filename = re.sub("\$\(TITLE\)", self.titleName, _db_filename)
        _db_filename = re.sub("\$\(AGENT\)", self.titleName, _db_filename)
        self.db_path = os.path.abspath(_db_filename)
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
            for id, item_buf, score in conn.execute(self._iterate_sql):
                yield bytes(item_buf)

    def _get_conn(self):
        id = get_ident()
        if id not in self._connection_cache:
            self._connection_cache[id] = sqlite3.Connection(self.db_path, timeout=60)
        return self._connection_cache[id]

    def _pop(self, get_sql, timeout=None, protective=False):
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
                    id, item_buf, score = next(cursor)
                    keep_polling = False
                except StopIteration:
                    # unlock the database
                    conn.commit()
                    now_timestamp = time.time()
                    if timeout is None or (now_timestamp - last_attempt_timestamp) >= timeout:
                        keep_polling = False
                        continue
                    tries += 1
                    time.sleep(wait)
                    wait = min(max_wait, tries / 10.0 + wait)
            if id is not None:
                if protective:
                    conn.execute(self._move_to_temp_sql, (id,))
                else:
                    conn.execute(self._pop_del_sql, (id,))
                conn.commit()
                return (id, bytes(item_buf), score)
        return None

    def _peek(self, peek_sql_template, skip_item=False, id=None, temporary=False):
        columns = "id, item, score"
        temp = 0
        if skip_item:
            columns = "id, score"
        if temporary:
            temp = 1
        peek_sql = peek_sql_template.format(columns=columns, temp=temp)
        with self._get_conn() as conn:
            if id is not None:
                cursor = conn.execute(peek_sql, (id,))
            else:
                cursor = conn.execute(peek_sql)
            try:
                if skip_item:
                    id, score = next(cursor)
                    return id, None, score
                else:
                    id, item_buf, score = next(cursor)
                    return id, bytes(item_buf), score
            except StopIteration:
                return None

    # number of objects in queue
    def size(self):
        return len(self)

    # enqueue with priority score
    def put(self, item, score):
        retVal = False
        item_buf = memoryviewOrBuffer(item)
        with self._get_conn() as conn:
            conn.execute(self._write_lock_sql)
            cursor = conn.execute(self._push_sql, (item_buf, score))
            n_row = cursor.rowcount
            if n_row == 1:
                retVal = True
        return retVal

    # enqueue by id
    def putbyid(self, id, item, score):
        retVal = False
        item_buf = memoryviewOrBuffer(item)
        with self._get_conn() as conn:
            cursor = conn.execute(self._push_by_id_sql, (id, item_buf, score))
            n_row = cursor.rowcount
            if n_row == 1:
                retVal = True
        return retVal

    # dequeue the first object
    def get(self, timeout=None, protective=False):
        sql_str = self._lpop_get_sql_template.format(columns="id, item, score")
        return self._pop(get_sql=sql_str, timeout=timeout, protective=protective)

    # dequeue the last object
    def getlast(self, timeout=None, protective=False):
        sql_str = self._rpop_get_sql_template.format(columns="id, item, score")
        return self._pop(get_sql=sql_str, timeout=timeout, protective=protective)

    # dequeue list of objects with some conditions
    def getmany(self, mode="first", minscore=None, maxscore=None, count=None, protective=False, temporary=False):
        temporary_str = "temporary = 1" if temporary else "temporary = 0"
        minscore_str = "" if minscore is None else "AND score >= {0}".format(float(minscore))
        maxscore_str = "" if maxscore is None else "AND score <= {0}".format(float(maxscore))
        count_str = "" if count is None else "LIMIT {0}".format(int(count))
        mode_rank_map = {
            "first": "",
            "last": "DESC",
        }
        get_many_sql = self._get_many_template.format(
            temporary_str=temporary_str, minscore_str=minscore_str, maxscore_str=maxscore_str, rank=mode_rank_map[mode], count_str=count_str
        )
        ret_list = []
        with self._get_conn() as conn:
            conn.execute(self._write_lock_sql)
            cursor = conn.execute(get_many_sql)
            ids = []
            for id, item_buf, score in cursor:
                ret_list.append((id, bytes(item_buf), score))
                ids.append(id)
            placeholders_str = ",".join("?" * len(ids))
            if protective:
                conn.execute(self._move_many_to_temp_sql_template.format(placeholders_str), ids)
            else:
                conn.execute(self._del_sql_template.format(placeholders_str), ids)
            conn.commit()
        return ret_list

    # get tuple of (id, item, score) of the first object without dequeuing it
    def peek(self, skip_item=False):
        return self._peek(self._lpop_get_sql_template, skip_item=skip_item)

    # get tuple of (id, item, score) of the last object without dequeuing it
    def peeklast(self, skip_item=False):
        return self._peek(self._rpop_get_sql_template, skip_item=skip_item)

    # get tuple of (id, item, score) of object by id without dequeuing it
    def peekbyid(self, id, temporary=False, skip_item=False):
        return self._peek(self._get_by_id_sql_template, skip_item=skip_item, id=id, temporary=temporary)

    # drop all objects in queue and index and reset primary key auto_increment
    def clear(self):
        with self._get_conn() as conn:
            conn.execute(self._exclusive_lock_sql)
            conn.execute(self._clear_drop_table_sql)
            try:
                conn.execute(self._clear_zero_id_sql)
            except sqlite3.OperationalError:
                pass
            conn.commit()
        self.__init__()

    # delete objects by list of id
    def delete(self, ids):
        if isinstance(ids, (list, tuple)):
            placeholders_str = ",".join("?" * len(ids))
            with self._get_conn() as conn:
                conn.execute(self._exclusive_lock_sql)
                cursor = conn.execute(self._del_sql_template.format(placeholders_str), ids)
            n_row = cursor.rowcount
            conn.commit()
            return n_row
        else:
            raise TypeError("ids should be list or tuple")

    # Move objects in temporary space to the queue
    def restore(self, ids):
        with self._get_conn() as conn:
            conn.execute(self._exclusive_lock_sql)
            if ids is None:
                conn.execute(self._restore_sql)
            elif isinstance(ids, (list, tuple)):
                placeholders_str = ",".join("?" * len(ids))
                conn.execute(self._restore_sql_template.format(placeholders_str), ids)
            else:
                raise TypeError("ids should be list or tuple or None")

    # update a object by its id with some conditions
    def update(self, id, item=None, score=None, temporary=None, cond_score=None):
        cond_score_str_map = {
            "gt": "AND score < ?",
            "ge": "AND score <= ?",
            "lt": "AND score > ?",
            "le": "AND score >= ?",
        }
        cond_score_str = cond_score_str_map.get(cond_score, "")
        attr_set_list = []
        params = []
        if item is not None:
            item_buf = memoryviewOrBuffer(item)
            attr_set_list.append("item = ?")
            params.append(item_buf)
        if score is not None:
            attr_set_list.append("score = ?")
            params.append(score)
        if temporary is not None:
            attr_set_list.append("temporary = ?")
            params.append(temporary)
        attr_set_str = " , ".join(attr_set_list)
        if not attr_set_str:
            return False
        sql_update = ("UPDATE OR IGNORE queue_table SET " "{attr_set_str} " "WHERE id = ? " "{cond_score_str} ").format(
            attr_set_str=attr_set_str, id=id, cond_score_str=cond_score_str
        )
        params.append(id)
        if cond_score_str:
            params.append(score)
        retVal = False
        with self._get_conn() as conn:
            cursor = conn.execute(sql_update, params)
            n_row = cursor.rowcount
            if n_row >= 1:
                retVal = True
        return retVal
