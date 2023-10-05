import time
import functools
import warnings

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config

warnings.simplefilter("ignore")


class MysqlFifo(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.reconnectTimeout = 300
        if hasattr(harvester_config, "fifo") and hasattr(harvester_config.fifo, "reconnectTimeout"):
            self.reconnectTimeout = harvester_config.db.reconnectTimeout
        elif hasattr(harvester_config.db, "reconnectTimeout"):
            self.reconnectTimeout = harvester_config.db.reconnectTimeout
        PluginBase.__init__(self, **kwarg)
        self.tableName = "{title}_FIFO".format(title=self.titleName)
        # get connection, cursor and error types
        self._connect_db()
        # create table for fifo
        try:
            self._make_table()
            # self._make_index()
            self.commit()
        except Exception as _e:
            self.rollback()
            raise _e

    # get connection, cursor and error types
    def _connect_db(self):
        # DB access attribues
        if hasattr(self, "db_host"):
            db_host = self.db_host
        else:
            try:
                db_host = harvester_config.fifo.db_host
            except AttributeError:
                db_host = "127.0.0.1"
        if hasattr(self, "db_port"):
            db_port = self.db_port
        else:
            try:
                db_port = harvester_config.fifo.db_port
            except AttributeError:
                db_port = 3306
        if hasattr(self, "db_user"):
            db_user = self.db_user
        else:
            db_user = harvester_config.fifo.db_user
        if hasattr(self, "db_password"):
            db_password = self.db_password
        else:
            db_password = harvester_config.fifo.db_password
        if hasattr(self, "db_schema"):
            db_schema = self.db_schema
        else:
            db_schema = harvester_config.fifo.db_schema
        try:
            import MySQLdb
            import MySQLdb.cursors
        except ImportError:
            try:
                import mysql.connector
            except ImportError:
                raise Exception("No available MySQL DB API installed. Please pip install mysqlclient or mysql-connection-python")
            else:
                self.con = mysql.connector.connect(user=db_user, passwd=db_password, db=db_schema, host=db_host, port=db_port, charset="utf8")
                self.cur = self.con.cursor(buffered=True)
                self.OperationalError = mysql.connector.errors.OperationalError
        else:
            self.con = MySQLdb.connect(user=db_user, passwd=db_password, db=db_schema, host=db_host, port=db_port)
            self.cur = self.con.cursor()
            self.OperationalError = MySQLdb.OperationalError

    # decorator exception handler for type of DBs
    def _handle_exception(method):
        def _decorator(_method, *args, **kwargs):
            @functools.wraps(_method)
            def _wrapped_method(self, *args, **kwargs):
                try:
                    _method(self, *args, **kwargs)
                except Exception as exc:
                    # Case to try renew connection
                    isOperationalError = False
                    if isinstance(exc, self.OperationalError):
                        isOperationalError = True
                    if isOperationalError:
                        try_timestamp = time.time()
                        n_retry = 1
                        while time.time() - try_timestamp < self.reconnectTimeout:
                            # close DB cursor
                            try:
                                self.cur.close()
                            except Exception as e:
                                pass
                            # close DB connection
                            try:
                                self.con.close()
                            except Exception as e:
                                pass
                            # restart the proxy instance
                            try:
                                self._connect_db()
                                return
                            except Exception as _e:
                                exc = _e
                                sleep_time = core_utils.retry_period_sec(n_retry, increment=2, max_seconds=300, min_seconds=1)
                                if not sleep_time:
                                    break
                                else:
                                    time.sleep(sleep_time)
                                    n_retry += 1
                        raise exc
                    else:
                        raise exc

            return _wrapped_method

        return _decorator(method)

    # wrapper for execute
    @_handle_exception
    def execute(self, sql, params=None):
        retVal = self.cur.execute(sql, params)
        return retVal

    # wrapper for executemany
    @_handle_exception
    def executemany(self, sql, params_list):
        retVal = self.cur.executemany(sql, params_list)
        return retVal

    # commit
    @_handle_exception
    def commit(self):
        self.con.commit()

    # rollback
    @_handle_exception
    def rollback(self):
        self.con.rollback()

    # make table
    def _make_table(self):
        sql_make_table = (
            "CREATE TABLE IF NOT EXISTS {table_name} "
            "("
            "  id BIGINT NOT NULL AUTO_INCREMENT,"
            "  item LONGBLOB,"
            "  score DOUBLE,"
            "  temporary TINYINT DEFAULT 0,"
            "  PRIMARY KEY (id) "
            ")"
        ).format(table_name=self.tableName)
        self.execute(sql_make_table)

    # make index
    def _make_index(self):
        sql_make_index = ("CREATE INDEX IF NOT EXISTS score_index ON {table_name} " "(score)").format(table_name=self.tableName)
        self.execute(sql_make_index)

    def _push(self, item, score):
        sql_push = ("INSERT INTO {table_name} " "(item, score) " "VALUES (%s, %s) ").format(table_name=self.tableName)
        params = (item, score)
        self.execute(sql_push, params)

    def _push_by_id(self, id, item, score):
        sql_push = ("INSERT IGNORE INTO {table_name} " "(id, item, score) " "VALUES (%s, %s, %s) ").format(table_name=self.tableName)
        params = (id, item, score)
        self.execute(sql_push, params)
        n_row = self.cur.rowcount
        if n_row == 1:
            return True
        else:
            return False

    def _pop(self, timeout=None, protective=False, mode="first"):
        sql_pop_get_first = ("SELECT id, item, score FROM {table_name} " "WHERE temporary = 0 " "ORDER BY score LIMIT 1 ").format(table_name=self.tableName)
        sql_pop_get_last = ("SELECT id, item, score FROM {table_name} " "WHERE temporary = 0 " "ORDER BY score DESC LIMIT 1 ").format(table_name=self.tableName)
        sql_pop_to_temp = ("UPDATE {table_name} SET temporary = 1 " "WHERE id = %s AND temporary = 0 ").format(table_name=self.tableName)
        sql_pop_del = ("DELETE FROM {table_name} " "WHERE id = %s AND temporary = 0 ").format(table_name=self.tableName)
        mode_sql_map = {
            "first": sql_pop_get_first,
            "last": sql_pop_get_last,
        }
        sql_pop_get = mode_sql_map[mode]
        keep_polling = True
        got_object = False
        _exc = None
        wait = 0.1
        max_wait = 2
        tries = 0
        id = None
        last_attempt_timestamp = time.time()
        while keep_polling:
            try:
                self.execute(sql_pop_get)
                res = self.cur.fetchall()
                if len(res) > 0:
                    id, item, score = res[0]
                    params = (id,)
                    if protective:
                        self.execute(sql_pop_to_temp, params)
                    else:
                        self.execute(sql_pop_del, params)
                    n_row = self.cur.rowcount
                    self.commit()
                    if n_row >= 1:
                        got_object = True
            except Exception as _e:
                self.rollback()
                _exc = _e
            else:
                if got_object:
                    keep_polling = False
                    return (id, item, score)
            now_timestamp = time.time()
            if timeout is None or (now_timestamp - last_attempt_timestamp) >= timeout:
                keep_polling = False
                if _exc is not None:
                    raise _exc
            tries += 1
            time.sleep(wait)
            wait = min(max_wait, tries / 10.0 + wait)
        return None

    def _peek(self, mode="first", id=None, skip_item=False):
        if skip_item:
            columns_str = "id, score"
        else:
            columns_str = "id, item, score"
        sql_peek_first = ("SELECT {columns} FROM {table_name} " "WHERE temporary = 0 " "ORDER BY score LIMIT 1 ").format(
            columns=columns_str, table_name=self.tableName
        )
        sql_peek_last = ("SELECT {columns} FROM {table_name} " "WHERE temporary = 0 " "ORDER BY score DESC LIMIT 1 ").format(
            columns=columns_str, table_name=self.tableName
        )
        sql_peek_by_id = ("SELECT {columns} FROM {table_name} " "WHERE id = %s AND temporary = 0 ").format(columns=columns_str, table_name=self.tableName)
        sql_peek_by_id_temp = ("SELECT {columns} FROM {table_name} " "WHERE id = %s AND temporary = 1 ").format(columns=columns_str, table_name=self.tableName)
        mode_sql_map = {
            "first": sql_peek_first,
            "last": sql_peek_last,
            "id": sql_peek_by_id,
            "idtemp": sql_peek_by_id_temp,
        }
        sql_peek = mode_sql_map[mode]
        try:
            if mode in ("id", "idtemp"):
                params = (id,)
                self.execute(sql_peek, params)
            else:
                self.execute(sql_peek)
            res = self.cur.fetchall()
            self.commit()
        except Exception as _e:
            self.rollback()
            raise _e
        if len(res) > 0:
            if skip_item:
                id, score = res[0]
                item = None
            else:
                id, item, score = res[0]
            return (id, item, score)
        else:
            return None

    def _update(self, id, item=None, score=None, temporary=None, cond_score=None):
        cond_score_str_map = {
            "gt": "AND score < %s",
            "ge": "AND score <= %s",
            "lt": "AND score > %s",
            "le": "AND score >= %s",
        }
        cond_score_str = cond_score_str_map.get(cond_score, "")
        attr_set_list = []
        params = []
        if item is not None:
            attr_set_list.append("item = %s")
            params.append(item)
        if score is not None:
            attr_set_list.append("score = %s")
            params.append(score)
        if temporary is not None:
            attr_set_list.append("temporary = %s")
            params.append(temporary)
        attr_set_str = " , ".join(attr_set_list)
        if not attr_set_str:
            return False
        sql_update = ("UPDATE IGNORE {table_name} SET " "{attr_set_str} " "WHERE id = %s " "{cond_score_str} ").format(
            table_name=self.tableName, attr_set_str=attr_set_str, cond_score_str=cond_score_str
        )
        params.append(id)
        if cond_score_str:
            params.append(score)
        try:
            self.execute(sql_update, params)
        except Exception as _e:
            self.rollback()
            raise _e
        n_row = self.cur.rowcount
        if n_row == 1:
            return True
        else:
            return False

    # number of objects in queue
    def size(self):
        sql_size = ("SELECT COUNT(id) FROM {table_name}").format(table_name=self.tableName)
        try:
            self.execute(sql_size)
            res = self.cur.fetchall()
        except Exception as _e:
            self.rollback()
            raise _e
        if len(res) > 0:
            return res[0][0]
        return None

    # enqueue with priority score
    def put(self, item, score):
        try:
            self._push(item, score)
            self.commit()
        except Exception as _e:
            self.rollback()
            raise _e

    # enqueue by id
    def putbyid(self, id, item, score):
        try:
            retVal = self._push_by_id(id, item, score)
            self.commit()
        except Exception as _e:
            self.rollback()
            raise _e
        else:
            return retVal

    # dequeue the first object
    def get(self, timeout=None, protective=False):
        return self._pop(timeout=timeout, protective=protective)

    # dequeue the last object
    def getlast(self, timeout=None, protective=False):
        return self._pop(timeout=timeout, protective=protective, mode="last")

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
        sql_get_many = (
            "SELECT id, item, score FROM {table_name} " "WHERE " "{temporary_str} " "{minscore_str} " "{maxscore_str} " "ORDER BY score {rank} " "{count_str} "
        ).format(
            table_name=self.tableName,
            temporary_str=temporary_str,
            minscore_str=minscore_str,
            maxscore_str=maxscore_str,
            rank=mode_rank_map[mode],
            count_str=count_str,
        )
        sql_pop_to_temp = ("UPDATE {table_name} SET temporary = 1 " "WHERE id = %s AND temporary = 0 ").format(table_name=self.tableName)
        sql_pop_del = ("DELETE FROM {table_name} " "WHERE id = %s AND temporary = {temporary} ").format(
            table_name=self.tableName, temporary=(1 if temporary else 0)
        )
        ret_list = []
        try:
            self.execute(sql_get_many)
            res = self.cur.fetchall()
            for _rec in res:
                got_object = False
                id, item, score = _rec
                params = (id,)
                if protective:
                    self.execute(sql_pop_to_temp, params)
                else:
                    self.execute(sql_pop_del, params)
                n_row = self.cur.rowcount
                self.commit()
                if n_row >= 1:
                    got_object = True
                if got_object:
                    ret_list.append(_rec)
        except Exception as _e:
            self.rollback()
            _exc = _e
        return ret_list

    # get tuple of (id, item, score) of the first object without dequeuing it
    def peek(self, skip_item=False):
        return self._peek(skip_item=skip_item)

    # get tuple of (id, item, score) of the last object without dequeuing it
    def peeklast(self, skip_item=False):
        return self._peek(mode="last", skip_item=skip_item)

    # get tuple of (id, item, score) of object by id without dequeuing it
    def peekbyid(self, id, temporary=False, skip_item=False):
        if temporary:
            return self._peek(mode="idtemp", id=id, skip_item=skip_item)
        else:
            return self._peek(mode="id", id=id, skip_item=skip_item)

    # get list of object tuples without dequeuing it
    def peekmany(self, mode="first", minscore=None, maxscore=None, count=None, skip_item=False):
        minscore_str = "" if minscore is None else "AND score >= {0}".format(float(minscore))
        maxscore_str = "" if maxscore is None else "AND score <= {0}".format(float(maxscore))
        count_str = "" if count is None else "LIMIT {0}".format(int(count))
        mode_rank_map = {
            "first": "",
            "last": "DESC",
        }
        if skip_item:
            columns_str = "id, score"
        else:
            columns_str = "id, item, score"
        sql_peek_many = (
            "SELECT {columns} FROM {table_name} " "WHERE temporary = 0 " "{minscore_str} " "{maxscore_str} " "ORDER BY score {rank} " "{count_str} "
        ).format(
            columns=columns_str, table_name=self.tableName, minscore_str=minscore_str, maxscore_str=maxscore_str, rank=mode_rank_map[mode], count_str=count_str
        )
        try:
            self.execute(sql_peek_many)
            res = self.cur.fetchall()
            self.commit()
        except Exception as _e:
            self.rollback()
            raise _e
        ret_list = []
        for _rec in res:
            if skip_item:
                id, score = _rec
                item = None
            else:
                id, item, score = _rec
            ret_list.append((id, item, score))
        return ret_list

    # drop all objects in queue and index and reset the table
    def clear(self):
        sql_clear_index = ("DROP INDEX IF EXISTS score_index ON {table_name} ").format(table_name=self.tableName)
        sql_clear_table = ("DROP TABLE IF EXISTS {table_name} ").format(table_name=self.tableName)
        # self.execute(sql_clear_index)
        try:
            self.execute(sql_clear_table)
        except Exception as _e:
            self.rollback()
            raise _e
        self.__init__()

    # delete objects by list of id
    def delete(self, ids):
        sql_delete_template = "DELETE FROM {table_name} WHERE id in ({placeholders} ) "
        if isinstance(ids, (list, tuple)):
            placeholders_str = ",".join([" %s"] * len(ids))
            sql_delete = sql_delete_template.format(table_name=self.tableName, placeholders=placeholders_str)
            try:
                self.execute(sql_delete, ids)
                n_row = self.cur.rowcount
                self.commit()
            except Exception as _e:
                self.rollback()
                raise _e
            return n_row
        else:
            raise TypeError("ids should be list or tuple")

    # Move objects in temporary space to the queue
    def restore(self, ids):
        if ids is None:
            sql_restore = ("UPDATE {table_name} SET temporary = 0 WHERE temporary != 0 ").format(table_name=self.tableName)
        elif isinstance(ids, (list, tuple)):
            placeholders_str = ",".join([" %s"] * len(ids))
            sql_restore = ("UPDATE {table_name} SET temporary = 0 " "WHERE temporary != 0 AND id in ({placeholders} ) ").format(
                table_name=self.tableName, placeholders=placeholders_str
            )
        else:
            raise TypeError("ids should be list or tuple or None")
        try:
            self.execute(sql_restore)
            self.commit()
        except Exception as _e:
            self.rollback()
            raise _e

    # update a object by its id with some conditions
    def update(self, id, item=None, score=None, temporary=None, cond_score=None):
        try:
            retVal = self._update(id, item, score, temporary, cond_score)
            self.commit()
        except Exception as _e:
            self.rollback()
            raise _e
        else:
            return retVal
