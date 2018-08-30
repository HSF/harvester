import os
import time
import re

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config

try:
    memoryviewOrBuffer = buffer
except NameError:
    memoryviewOrBuffer = memoryview


class MysqlFifo(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.tableName = '{agent}-fifo'.format(agent=self.agentName)
        # DB access attribues
        if hasattr(self, 'db_host'):
            db_host = self.db_host
        else:
            try:
                db_host = harvester_config.fifo.db_host
            except AttributeError:
                db_host = '127.0.0.1'
        if hasattr(self, 'db_port'):
            db_port = self.db_port
        else:
            try:
                db_port = harvester_config.fifo.db_port
            except AttributeError:
                db_port = 3306
        if hasattr(self, 'db_user'):
            db_user = self.db_user
        else:
            db_user = harvester_config.fifo.db_user
        if hasattr(self, 'db_password'):
            db_password = self.db_password
        else:
            db_password = harvester_config.fifo.db_password
        if hasattr(self, 'db_schema'):
            db_schema = self.db_schema
        else:
            db_schema = harvester_config.fifo.db_schema
        # get connection, cursor and error types
        try:
            import MySQLdb
            import MySQLdb.cursors
        except ImportError:
            try:
                import mysql.connector
            except ImportError:
                raise Exception('No available MySQL DB API installed. Please pip install mysqlclient or mysql-connection-python')
            else:
                self.con = mysql.connector.connect(user=db_user, passwd=db_password,
                                                    db=db_schema, host=db_host, port=db_port)
                self.cur = self.con.cursor(buffered=True)
                self.OperationalError = mysql.connector.errors.OperationalError
        else:
            self.con = MySQLdb.connect(user=db_user, passwd=db_password,
                                        db=db_schema, host=db_host, port=db_port)
            self.cur = self.con.cursor()
            self.OperationalError = MySQLdb.OperationalError
        # create table for fifo
        try:
            self._make_table()
            self._make_index()
            self.commit()
        except Exception as _e:
            self.rollback()
            # raise _e


    # exception handler for type of DBs
    def _handle_exception(self, exc, retry_time=30):
        # Case to try renew connection
        isOperationalError = False
        if isinstance(exc, self.OperationalError):
            isOperationalError = True
        if isOperationalError:
            try_timestamp = time.time()
            while time.time() - try_timestamp < retry_time:
                try:
                    self.__init__()
                    return
                except Exception as _e:
                    time.sleep(1)
            raise _e
        else:
            raise exc

    # wrapper for execute
    def execute(self, sql, params=None):
        try:
            # execute
            retVal = self.cur.execute(sql, params)
        # return
        return retVal

    # wrapper for executemany
    def executemany(self, sql, params_list):
        # get lock
        try:
            # execute
            retVal = self.cur.executemany(sql, params_list)
        # return
        return retVal

    # commit
    def commit(self):
        try:
            self.con.commit()
        except Exception as e:
            self._handle_exception(e)

    # rollback
    def rollback(self):
        try:
            self.con.rollback()
        except Exception as e:
            self._handle_exception(e)

    # make table
    def _make_table(self):
        sql_make_table = (
                'CREATE TABLE IF NOT EXISTS {table_name} '
                '('
                '  id BIGINT PRIMARY KEY,'
                '  item BLOB,'
                '  score FLOAT,'
                '  temporary TINYINT DEFAULT 0 '
                ')'
                ).format(table_name=self.tableName)
        self.execute(sql_make_table)

    # make index
    def _make_index(self):
        sql_make_index = (
                'CREATE INDEX IF NOT EXISTS score_index ON {table_name} '
                '(score)'
            ).format(table_name=self.tableName)
        self.execute(sql_make_index)

    def _push(self, obj, score):
        # obj_buf = memoryviewOrBuffer(obj)
        # with self._get_conn() as conn:
        #     conn.execute(push_sql, (obj_buf, score))
        sql_push = (
                'INSERT INTO {table_name} '
                '(item, score) '
                'VALUES (%s, %f) '
            ).format(table_name=self.tableName)
        params = (obj, score)
        self.execute(sql_push, params)

    def _pop(self, timeout=None, protective=False):
        sql_pop_get = (
                'SELECT id, item, score FROM {table_name} '
                'WHERE temporary = 0 '
                'ORDER BY score LIMIT 1 '
            ).format(table_name=self.tableName)
        sql_pop_to_temp = (
                'UPDATE {table_name} SET temporary = 1 WHERE id = %d'
            ).format(table_name=self.tableName)
        sql_pop_del = (
                'DELETE FROM {table_name} WHERE id = %d'
            ).format(table_name=self.tableName)
        keep_polling = True
        wait = 0.1
        max_wait = 2
        tries = 0
        last_attempt_timestamp = time.time()
        id = None
        while keep_polling:
            try:
                self.execute(sql_pop_get)
                res = self.cur.fetchone()
                if res is not None:
                    id, obj, score = res
                    params = (id,)
                    if protective:
                        self.execute(sql_pop_to_temp, params)
                    else:
                        self.execute(sql_pop_del, params)
                    self.commit()
            except Exception as _e:
                self.rollback()
                now_timestamp = time.time()
                if timeout is None or (now_timestamp - last_attempt_timestamp) >= timeout:
                    keep_polling = False
                    # raise _e
                    # continue
            else:
                keep_polling = False
                return (id, obj, score)
            tries += 1
            time.sleep(wait)
            wait = min(max_wait, tries/10.0 + wait)
        return None

    # number of objects in queue
    def size(self):
        sql_size = (
                'SELECT COUNT(id) FROM {table_name}'
            ).format(table_name=self.tableName)
        res = self.cur.fetchone()
        if res is not None:
            return res[0]
        return None

    # enqueue with priority score
    def put(self, obj, score):
        try:
            self._push(obj, score)
            self.commit()
        except Exception as _e:
            self.rollback()
            # raise _e

    # dequeue the first object
    def get(self, timeout=None, protective=False):
        return self._pop(timeout=timeout, protective=protective)

    # get tuple of (item, score) of the first object without dequeuing it
    def peek(self):
        sql_peek = (
                'SELECT id, item, score FROM {table_name} '
                'WHERE temporary = 0 '
                'ORDER BY score LIMIT 1 '
            ).format(table_name=self.tableName)
        self.execute(sql_pop_get)
        res = self.cur.fetchone()
        if res is not None:
            id, obj, score = res
            return (obj, score)
        else:
            return (None, None)

    # drop all objects in queue and index and reset the table
    def clear(self):
        sql_clear = (
                'DROP TABLE IF EXISTS {table_name} '
            ).format(table_name=self.tableName)
        self.execute(sql_clear)
        self.__init__()

    # delete an object by list of id
    def delete(self, ids):
        sql_delete_template = (
                'DELETE FROM {table_name} WHERE id in ({0} ) '
            ).format(table_name=self.tableName)
        if isinstance(ids, (list, tuple)):
            placeholders_str = ','.join(' %d' * len(ids))
            sql_delete = sql_delete_template.format(placeholders_str)
            self.execute(sql_delete, ids)
            self.commit()
        else:
            raise TypeError('ids should be list or tuple')

    # Move all object in temporary space to the queue
    def restore(self):
        sql_restore = (
                'UPDATE {table_name} SET temporary = 0 WHERE temporary != 0 '
            ).format(table_name=self.tableName)
        try:
            self.execute(sql_restore)
            self.commit()
        except Exception as _e:
            self.rollback()
            # raise _e
