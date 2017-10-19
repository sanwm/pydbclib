from collections import OrderedDict
import sys
# import json
# from py_db.utils import ObjEncoder
from py_db.utils import reduce_num
from py_db.mylogger import log


class Connection(object):
    # driver_name = ""
    # @classmethod
    # def _import_module(cls, name):
    #     n = name.rfind('.')
    #     if n == -1:
    #         return __import__(name)
    #     else:
    #         subname = name[n + 1:]
    #         return getattr(cls._import_module(name[:n]), subname)

    # @classmethod
    # def dbapi(cls):
    #     __import__(cls.driver_name)
    #     return sys.modules[cls.driver_name]
        # return cls._import_module(cls.driver_name)
        # return __import__(cls.driver_name, globals(), locals(), [], -1)

    def __init__(self, *args, **kwargs):
        # print(args, kwargs)
        self.driver_name = kwargs.get("driver")
        kwargs.pop('driver')
        # print(args, kwargs)
        self.create_driver()
        self.connect = self.create_con(*args, **kwargs)
        self.session = self.create_session()

    def dbapi(self):
        __import__(self.driver_name)
        return sys.modules[self.driver_name]

    def create_driver(self):
        self.driver = self.dbapi()
        self.DatabaseError = self.driver.DatabaseError

    def create_con(self, *args, **kwargs):
        return self.driver.connect(*args, **kwargs)

    def create_session(self):
        return self.connect.cursor()

    def execute(self, sql, *args, **kw):
        if ":1" in sql and '?' not in sql:
            sql = sql.replace(":1", "?")
            # print(sql)
        if args:
            log.debug("%s %s" % (sql, args[0]))
        else:
            log.debug(sql)
        return self.session.execute(sql, *args, **kw)

    def executemany(self, sql, *args, **kw):
        if ":1" in sql and '?' not in sql:
            sql = sql.replace(":1", "?")
        log.debug("%s [%s...%s]" % (sql, args[0][0], args[0][-1]))
        return self.session.executemany(sql, *args, **kw)

    def _query_generator(self, sql, args, chunksize):
        self.execute(sql, args)
        res = self.session.fetchmany(chunksize)
        while res:
            yield res
            res = self.session.fetchmany(chunksize)

    def query(self, sql, args=[], size=None):
        if size is None:
            self.execute(sql, args)
            res = [tuple(i) for i in self.session.fetchall()]
            return res
        else:
            return self._query_generator(sql, args, size)

    def _query_dict_generator(self, sql, Dict, args, chunksize):
        self.execute(sql, args)
        colunms = [i[0].lower() for i in self.description()]
        res = self.session.fetchmany(chunksize)
        while res:
            yield [Dict(zip(colunms, i)) for i in res]
            res = self.session.fetchmany(chunksize)

    def query_dict(self, sql, args=[], ordered=False, size=None):
        Dict = OrderedDict if ordered else dict
        if size is None:
            self.execute(sql, args)
            colunms = [i[0].lower() for i in self.description()]
            return [Dict(zip(colunms, i)) for i in self.session.fetchall()]
        else:
            return self._query_dict_generator(sql, Dict, args, size)

    def insert(self, sql, args=[], num=10000):
        length = len(args)
        count = 0
        # log.info(args, length)
        try:
            if (args and not isinstance(args, dict) and
                    isinstance(args[0], (tuple, list, dict))):
                for i in range(0, length, num):
                    self.executemany(sql, args[i:i + num])
                    count += self.session.rowcount
            else:
                self.execute(sql, args)
                count = self.session.rowcount
        except self.DatabaseError as reason:
            self.rollback()
            if (args and not isinstance(args, dict) and
                    isinstance(args[0], (tuple, list, dict))):
                if num <= 10 or length <= 10:
                    # err_msg = ['SQL EXECUTEMANY ERROR\n %s\n' % sql]
                    # err_msg.append(json.dumps(
                    #     args[i:i + num], cls=ObjEncoder, indent=1))
                    # err_msg.append('\nSQL EXECUTEMANY ERROR')
                    # log.error(''.join(err_msg))
                    # log.error(reason)
                    # sys.exit()
                    log.error("SQL EXECUTEMANY ERROR"
                              "begin execute for everyone")
                    for record in args[i:i + num]:
                        self.insert(sql, record)
                else:
                    self.insert(
                        sql, args[i:i + num],
                        num=reduce_num(num, length))
            else:
                log.error(
                    'SQL EXECUTE ERROR\n%s\n%s' %
                    (sql, args)
                )
                log.error(reason)
                sys.exit()
        return count

    def merge(self, table, args, columns, unique, num=10000):
        param_columns = ','.join([':{0} as {0}'.format(i) for i in columns])
        update_field = ','.join(
            ['t1.{0}=t2.{0}'.format(i) for i in columns if i != unique])
        t1_columns = ','.join(['t1.{0}'.format(i) for i in columns])
        t2_columns = ','.join(['t2.{0}'.format(i) for i in columns])
        sql = ("MERGE INTO {table} t1"
               " USING (SELECT {param_columns} FROM dual) t2"
               " ON (t1.{unique}= t2.{unique})"
               " WHEN MATCHED THEN"
               " UPDATE SET {update_field}"
               " WHEN NOT MATCHED THEN"
               " INSERT ({t1_columns})"
               " VALUES ({t2_columns})".format(table=table,
                                               param_columns=param_columns,
                                               unique=unique,
                                               update_field=update_field,
                                               t1_columns=t1_columns,
                                               t2_columns=t2_columns))
        # log.info(sql)
        self.execute(sql, args)

    def delete_repeat(self, table, unique, cp_field="rowid"):
        """
        数据去重
        默认通过rowid方式去重
        """
        sql = "delete from {table} where {cp_field} is null".format(
            table=table, cp_field=cp_field)
        self.execute(sql)
        null_count = self.session.rowcount
        log.info(
            '删除对比字段(%s)中为空的数据：%s' % (cp_field, null_count)
        ) if null_count else None
        sql = ("delete from {table} where"
               " ({id}) in (select {id} from {table} GROUP BY {id}"
               " HAVING count({one_of_id})>1) and ({id},{cp_field}) not in"
               " (select {id},max({cp_field}) from {table} GROUP BY {id}"
               " HAVING count({one_of_id})>1)".format(
                   table=table, id=unique, cp_field=cp_field,
                   one_of_id=unique.split(',')[0]))
        self.execute(sql)
        count = self.session.rowcount
        log.info('删除重复数据：%s' % count)
        return count

    def description(self):
        return self.session.description

    def rollback(self):
        """
        数据回滚
        """
        self.connect.rollback()

    def commit(self):
        """
        提交
        """
        self.connect.commit()

    def close(self):
        """
        关闭数据库连接
        """
        self.session.close()
        self.connect.close()

    def __enter__(self):
        return self

    def __exit__(self, exctype, excvalue, traceback):
        try:
            if exctype is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()


# class OracleConnection(Connection):
#     driver_name = "cx_Oracle"


# class OdbcConnection(Connection):
#     driver_name = "pyodbc"


# class ImpalaConnection(Connection):
#     driver_name = "impala.dbapi"


# def db_test():
#     # db = OracleConnection('jwdn/password@local:1521/xe')
#     db = OdbcConnection('DSN=mydb;UID=root;PWD=password')
#     res = db.query("select * from TEST")
#     print(res)
#     db = ImpalaConnection(
#         host='172.16.17.18', port=21050,
#         use_kerberos=True, kerberos_service_name="impala",
#         timeout=3600)
#     # res = db.query_dict('show databases')
#     # print(res)
#     # db.execute('use sjck')
#     # res = db.query('show tables')
#     # print(res)
#     # sql = "select * from dc_dict limit ?"
#     # print(db.query(sql, [1]))


# if __name__ == "__main__":
#     db_test()
