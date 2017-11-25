from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DatabaseError, DBAPIError
from collections import OrderedDict
from sqlalchemy import engine
import re
import os
import sys
from py_db.utils import reduce_num
from py_db.sql import handle
from py_db.logger import log
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class Connection(object):

    def __init__(self, con, echo=False):
        """
        >>>con
        "oracle+cx_oracle://jwdn:password@192.168.152.1:1521/xe"
        """
        self.connect = con if isinstance(
            con, engine.base.Engine) else create_engine(con, echo=echo)
        self.session = self.create_session()

    def create_session(self):
        DB_Session = sessionmaker(bind=self.connect)
        return DB_Session()

    # def execute(self, sql, args=[], num=10000):
    #     """
    #     执行sql
    #     """
    #     if (':1' in sql and args and isinstance(args, (list, tuple)) and
    #             not isinstance(args[0], dict)):
    #         num = sql.count(':1')
    #         sql = sql.replace(':1', '{}').format(
    #             *[':%s' % i for i in range(num)])
    #         keys = [str(i) for i in range(num)]
    #         if isinstance(args[0], (list, tuple)):
    #             args = [dict(zip(keys, i)) for i in args]
    #         else:
    #             args = dict(zip(keys, args))
    #     if (args and not isinstance(args, dict) and
    #             isinstance(args[0], (list, tuple, dict))):
    #         rs = self.executemany(sql, args, num)
    #     else:
    #         rs = self.executeone(sql, args)
    #     return rs
    def execute(self, sql, args=[], num=10000):
        """
        执行sql
        """
        is_list = (':' in sql and args and isinstance(args, (list, tuple)) and
            not isinstance(args[0], dict))
        is_many = (args and not isinstance(args, dict) and
            isinstance(args[0], (list, tuple, dict)))
        if is_list:
            sql, keys = handle(sql)
            if isinstance(args[0], (list, tuple)):
                args = [dict(zip(keys, i)) for i in args]
            else:
                args = dict(zip(keys, args))
        if is_many:
            rs = self.executemany(sql, args, num)
        else:
            rs = self.executeone(sql, args)
        return rs

    def executeone(self, sql, args):
        try:
            rs = self.session.execute(sql, args)
            if args:
                log.debug("%s\nParam:%s" % (sql, args))
            else:
                log.debug(sql)
        except (DatabaseError, DBAPIError) as reason:
            self.rollback()
            if args:
                log.error(
                    'SQL EXECUTE ERROR(SQL: "%s")\nParam:%s' % (sql, args))
            else:
                log.error('SQL EXECUTE ERROR(SQL: "%s")' % sql)
            log.critical("REASON {%s}\nEXIT" % reason)
            sys.exit(1)
        return rs

    def executemany(self, sql, args, num):
        length = len(args)
        count = 0
        try:
            for i in range(0, length, num):
                rs = self.session.execute(sql, args[i:i + num])
                if len(args) > 2:
                    log.debug(
                        "%s\nParam:[%s\n           ..."
                        "\n       %s]" % (sql, args[0], args[-1]))
                else:
                    log.debug("%s\nParam:[%s]" % (
                        sql, '\n       '.join(map(str, args))))
                count += rs.rowcount
        except (DatabaseError, DBAPIError) as reason:
            self.rollback()
            if num <= 10 or length <= 10:
                log.warn("SQL EXECUTEMANY ERROR EXECUTE EVERYONE")
                for record in args[i:i + num]:
                    rs = self.executeone(sql, record)
            else:
                self.executemany(
                    sql, args[i:i + num],
                    num=reduce_num(num, length))
        rs.rowcount = count
        return rs

    def _query_generator(self, sql, args, chunksize):
        rs = self.execute(sql, args)
        res = rs.fetchmany(chunksize)
        while res:
            yield res
            res = rs.fetchmany(chunksize)

    def query(self, sql, args=[], size=None):
        if size is None:
            rs = self.execute(sql, args).fetchall()
            res = [tuple(i) for i in rs]
            return res
        else:
            return self._query_generator(sql, args, size)

    def _query_dict_generator(self, sql, Dict, args, chunksize):
        rs = self.execute(sql, args)
        colunms = [i[0].lower() for i in rs._cursor_description()]
        res = rs.fetchmany(chunksize)
        while res:
            yield [Dict(zip(colunms, i)) for i in res]
            res = rs.fetchmany(chunksize)

    def query_dict(self, sql, args=[], ordered=False, size=None):
        Dict = OrderedDict if ordered else dict
        if size is None:
            rs = self.execute(sql)
            colunms = [i[0].lower() for i in rs._cursor_description()]
            return [Dict(zip(colunms, i)) for i in rs.fetchall()]
        else:
            return self._query_dict_generator(sql, Dict, args, size)

    def _modify_field_size(self, reason):
        """
        修改oracle char 类型字段大小
        """
        match_reason = re.compile(
            r'"\w+"\."(\w+)"\."(\w+)" 的值太大 \(实际值: (\d+),')
        rs = match_reason.search(str(reason))
        table_name, column_name, value = (
            rs.group(1), rs.group(2), rs.group(3))
        delta = 2 if len(
            str(value)) == 1 else 2 * 10**(len(str(value)) - 2)
        sql_query = ("select data_type,char_used from user_tab_columns"
                     " where table_name='%s' and column_name='%s'" % (
                         table_name, column_name))
        data_type, char_used = self.query(sql_query)[0]
        if char_used == 'C':
            char_type = 'char'
        elif char_used == 'B':
            char_type = 'byte'
        else:
            char_type = ''
        sql_modify = ("alter table {table_name} modify({column_name}"
                      " {data_type}({value} {char_type}))".format(
                          table_name=table_name,
                          column_name=column_name,
                          data_type=data_type,
                          char_type=char_type,
                          value=int(value) + delta))
        self.execute(sql_modify)

    # def insert(self, sql, args=[], num=10000):
    #     """
    #     批量更新插入，默认超过10000条数据自动commit
    #     insertone:
    #         self.insert(
    #             "insert into test(id) values(:id)",
    #             {'id': 6666}
    #         )
    #     insertmany:
    #         self.insert(
    #             "insert into test(id) values(:id)",
    #             [{'id': 6666}, {'id': 8888}]
    #         )
    #     @num:
    #         批量插入时定义一次插入的数量，默认10000
    #     """
    #     length = len(args)
    #     count = 0
    #     try:
    #         if (args and not isinstance(args, dict) and
    #                 isinstance(args[0], (tuple, list, dict))):
    #             for i in range(0, length, num):
    #                 rs = self.execute(sql, args[i:i + num])
    #                 count += rs.rowcount
    #         else:
    #             rs = self.execute(sql, args)
    #             count = rs.rowcount
    #     except DatabaseError as reason:
    #         self.rollback()
    #         # if 0:
    #         #     pass
    #         if 'ORA-12899' in str(reason):
    #             self._modify_field_size(reason)
    #             count += self.insert(sql, args, num)
    #         else:
    #             if (args and not isinstance(args, dict) and
    #                     isinstance(args[0], (tuple, list, dict))):
    #                 if reduce_num(num, length) <= 10 or length <= 10:
    #                     log.error("SQL EXECUTEMANY ERROR EXECUTE EVERYONE")
    #                     for record in args[i:i + num]:
    #                         self.insert(sql, record)
    #                 else:
    #                     self.insert(
    #                         sql, args[i:i + num],
    #                         num=reduce_num(num, length))
    #             else:
    #                 log.error(
    #                     'SQL EXECUTE ERROR\n%s\n%s' %
    #                     (sql, args))
    #                 log.error(reason)
    #                 sys.exit()
    #     return count
    def insert(self, sql, args=[], num=10000):
        rs = self.execute(sql, args, num)
        return rs.rowcount

    def merge(self, table, args, unique, num=10000, db_type=None):
        if (not args or not isinstance(args, (tuple, list))
                or not isinstance(args[0], dict)):
            log.error("args 形式错误，必须是字典集合 for example([{'a':1},{'b':2}])")
        db = db_type.lower() if db_type else None
        columns = [i for i in args[0].keys()]
        if db != "oracle":
            param = []
            keys = []
            for r in args:
                param.append([r[j] for j in columns])
                keys.append([r[unique]])
        if db == "oracle":
            self.oracle_merge(table, args, columns, unique, num)
        elif db == "mysql":
            self.mysql_merge(table, param, columns, unique, num)
        elif db == "postgressql":
            self.postgressql_merge(table, param, columns, unique, num)
        else:
            # log.error('%s database do not supper merge method')
            # sys.exit(1)
            self.common_merge(table, param, columns, keys, unique, num)

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
        values = ','.join([':1' for _ in columns])
        update_field = ','.join(["%s=:1" % i for i in columns if i != unique])
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
        values = ','.join([':1' for _ in columns])
        update_field = ','.join(["%s=:1" % i for i in columns if i != unique])
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

    def common_merge(self, table, args, columns, keys, unique, num=10000):
        columns = [i.lower() for i in columns]
        unique = unique.lower()
        self.execute("delete from %s where %s=:1" % (table, unique), keys)
        values = ','.join([':1' for _ in columns])
        sql = ("INSERT INTO {table}({columns}) "
               "VALUES({values}) ".format(
            table=table, unique=unique,
            columns=','.join(columns),
            values=values))
        self.execute(sql, args)

    def delete_repeat(self, table, unique, cp_field="rowid"):
        """
        数据去重
        默认通过rowid方式去重
        """
        sql = "delete from {table} where {cp_field} is null".format(
            table=table, cp_field=cp_field)
        null_count = self.execute(sql).rowcount
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
        rs = self.execute(sql)
        count = rs.rowcount
        log.info('删除重复数据：%s' % count)
        return count

    def empty(self, table):
        sql = "truncate table %s" % table
        db.insert(sql)

    def rollback(self):
        """
        数据回滚
        """
        self.session.rollback()

    def commit(self):
        """
        提交
        """
        self.session.commit()

    def close(self):
        """
        关闭数据库连接
        """
        self.session.close()

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


def db_test():
    import logging
    log.setLevel(logging.DEBUG)
    db_uri = "oracle+cx_oracle://jwdn:password@local:1521/xe"
    with Connection(db_uri, echo=True) as db:
        sql = "insert into test(foo,bar) values(:1,:1)"
        print(db.insert(sql, [('aaa', 'bbb'), ('aaa', 'bbb'), ('aaa', 'bbb')]))
        sql = "insert into test(foo,bar) values(:a,:b)"
        print(db.insert(sql, {'a': 'AAA', 'b': 'BBB'}))
        # print(db.delete_repeat('test', 'id', 'dtime'))
        # db.merge('test', {'foo': '1', 'id': 2222}, ['foo', 'id'], 'foo')


if __name__ == "__main__":
    db_test()
