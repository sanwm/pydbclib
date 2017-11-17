from collections import OrderedDict
import sys
import os
from py_db.utils import reduce_num
from py_db.logger import log
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class Connection(object):

    def __init__(self, *args, **kwargs):
        self.driver_name = kwargs.get("driver")
        kwargs.pop('driver')
        self.placeholder = kwargs.get("placeholder", '?')
        kwargs.pop('placeholder', None)
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
        try:
            self.Error = self.driver.Error
        except AttributeError:
            self.Error = self.DatabaseError

    def create_con(self, *args, **kwargs):
        try:
            con = self.driver.connect(*args, **kwargs)
            # print(dir(con))
            # print(con.module)
        except Exception as reason:
            log.critical("REASON(%s)\nargs:%s, kwargs:%s\nEXIT" % (
                reason, args, kwargs))
            sys.exit(1)
        return con

    def create_session(self):
        return self.connect.cursor()

    def execute(self, sql, args=[], num=10000):
        if (":1" in sql and '?' not in sql and
                self.driver_name != "cx_Oracle"):
            sql = sql.replace(":1", self.placeholder)
        if (args and not isinstance(args, dict) and
                isinstance(args[0], (tuple, list, dict))):
            count = self.executemany(sql, args, num)
            # if len(args) > 2:
            #     log.debug(
            #         "%s\nParam:[%s\n           ..."
            #         "\n       %s]" % (sql, args[0], args[-1]))
            # else:
            #     log.debug("%s\nParam:[%s]" % (
            #         sql, '\n           '.join(map(str, args))))
        else:
            count = self.executeone(sql, args)
            # if args:
            #     log.debug("%s\nParam:%s" % (sql, args))
            # else:
            #     log.debug(sql)
        return count

    def executeone(self, sql, args):
        try:
            self.session.execute(sql, args)
            if args:
                log.debug("%s\nParam:%s" % (sql, args))
            else:
                log.debug(sql)
            count = self.session.rowcount
        except (self.DatabaseError, self.Error) as reason:
            self.rollback()
            if args:
                log.error(
                    'SQL EXECUTE ERROR(SQL: "%s")\nParam:%s' % (sql, args))
            else:
                log.error('SQL EXECUTE ERROR(SQL: "%s")' % sql)
            log.critical("REASON {%s}\nEXIT" % reason)
            sys.exit(1)
        return count

    def executemany(self, sql, args, num):
        length = len(args)
        count = 0
        try:
            for i in range(0, length, num):
                self.session.executemany(sql, args[i:i + num])
                if len(args) > 2:
                    log.debug(
                        "%s\nParam:[%s\n           ..."
                        "\n       %s]" % (sql, args[0], args[-1]))
                else:
                    log.debug("%s\nParam:[%s]" % (
                        sql, '\n       '.join(map(str, args))))
                count += self.session.rowcount
        except (self.DatabaseError, self.Error) as reason:
            self.rollback()
            if num <= 10 or length <= 10:
                log.warn(reduce_num(num, length))
                log.warn("SQL EXECUTEMANY ERROR EXECUTE EVERYONE")
                for record in args[i:i + num]:
                    self.executeone(sql, record)
            else:
                self.executemany(
                    sql, args[i:i + num],
                    num=reduce_num(num, length))
        return count

    def _query_generator(self, sql, args, chunksize):
        self.execute(sql, args)
        res = self.session.fetchmany(chunksize)
        while res:
            yield res
            res = self.session.fetchmany(chunksize)

    def query(self, sql, args=[], size=None):
        """
        Args:
            sql: str
            args: list or dict
        Returns:
            list
        Raises:
            None
        """
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

    # def insert(self, sql, args=[], num=10000):
    #     length = len(args)
    #     count = 0
    #     try:
    #         if (args and not isinstance(args, dict) and
    #                 isinstance(args[0], (tuple, list, dict))):
    #             for i in range(0, length, num):
    #                 self.execute(sql, args[i:i + num])
    #                 count += self.session.rowcount
    #         else:
    #             self.execute(sql, args)
    #             count = self.session.rowcount
    #     except self.DatabaseError as reason:
    #         self.rollback()
    #         if (args and not isinstance(args, dict) and
    #                 isinstance(args[0], (tuple, list, dict))):
    #             if reduce_num(num, length) <= 10 or length <= 10:
    #                 log.error("SQL EXECUTEMANY ERROR EXECUTE EVERYONE")
    #                 for record in args[i:i + num]:
    #                     self.insert(sql, record)
    #             else:
    #                 self.insert(
    #                     sql, args[i:i + num],
    #                     num=reduce_num(num, length))
    #         else:
    #             log.error(
    #                 'SQL EXECUTE ERROR\n%s\n%s' %
    #                 (sql, args))
    #             print(type(str(reason)))
    #             raise ValueError()
    #             # sys.exit(0)
    #     return count
    def insert(self, sql, args=[], num=10000):
        return self.execute(sql, args, num)

    def merge(self, table, args, unique, num=10000, db_type="oracle"):
        if (not args or not isinstance(args, (tuple, list))
                or not isinstance(args[0], dict)):
            log.error("args 形式错误，必须是字典的list集合 for example([{'id':1},{'name':'te'}])")
        param = []
        columns = [i for i in args[0].keys()]
        for r in args:
            param.append([r[j] for j in columns])
        db = db_type.lower()
        if db == "oracle":
            self.oracle_merge(table, param, columns, unique, num)
        elif db == "mysql":
            self.mysql_merge(table, param, columns, unique, num)
        elif db == "postgressql":
            self.postgressql_merge(table, param, columns, unique, num)
        else:
            log.error('%s database do not supper merge method')
            sys.exit(1)

    def oracle_merge(self, table, args, columns, unique, num=10000):
        columns = [i.lower() for i in columns]
        unique = unique.lower()
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
        self.execute(sql, args)

    def mysql_merge(self, table, args, columns, unique, num=10000):
        columns = [i.lower() for i in columns]
        unique = unique.lower()
        param = []
        values = ','.join([self.placeholder for _ in columns])
        update_field = ','.join(["%s=%s" % (i, self.placeholder) for i in columns if i != unique])
        sql = ("INSERT INTO {table}({columns}) "
               "VALUES({values}) "
               "ON DUPLICATE KEY "
               "UPDATE {update_field}".format(table=table,
                                              columns=','.join(columns),
                                              values=values,
                                              update_field=update_field))
        idx = columns.index(unique)
        idx_list = [i for i in range(len(columns)) if i != idx]
        for record in args:
            for i in idx_list:
                record = list(record)
                record.append(record[i])
            param.append(record)
        self.execute(sql, param)

    def postgressql_merge(self, table, args, columns, unique, num=10000):
        columns = [i.lower() for i in columns]
        unique = unique.lower()
        param = []
        values = ','.join([self.placeholder for _ in columns])
        update_field = ','.join(["%s=%s" % (i, self.placeholder) for i in columns if i != unique])
        sql = ("INSERT INTO {table}({columns}) "
               "VALUES({values}) "
               "ON conflict({unique})"
               "DO UPDATE SET {update_field}".format(table=table, unique=unique,
                                                     columns=','.join(columns),
                                                     values=values,
                                                     update_field=update_field))
        idx = columns.index(unique)
        idx_list = [i for i in range(len(columns)) if i != idx]
        for record in args:
            for i in idx_list:
                record = list(record)
                record.append(record[i])
            param.append(record)
        self.execute(sql, param)

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

    def empty(self, table):
        sql = "truncate table %s" % table
        db.insert(sql)

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
