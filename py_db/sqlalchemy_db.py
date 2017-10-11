from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DatabaseError
from collections import OrderedDict
from sqlalchemy import engine
import json
import re
import os
if __name__ == '__main__':
    import sys
    sys.path.insert(
        0,
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    )
    from mylogger import log
else:
    from .mylogger import log
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def print(*args, notice='print values'):
    log.debug('%s>>>>>>\n%s' % (notice, ' '.join(['%s' % i for i in args])))


def reduce_num(n, l):
    num = min(n, l)
    if num > 10:
        return num % 10
    else:
        return 1


class Connection():

    def __init__(self, con, echo=False):
        """
        >>>con
        "oracle+cx_oracle://jwdn:password@192.168.152.1:1521/xe"
        """
        self._eng = con if isinstance(
            con, engine.base.Engine) else create_engine(con, echo=echo)
        self.session = self.create_session()

    def create_session(self):
        DB_Session = sessionmaker(bind=self._eng)
        return DB_Session()

    def execute(self, sql, args=None):
        """
        执行sql
        """
        if ':1' in sql:
            num = sql.count(':1')
            sql = sql.replace(':1', '%s') % tuple(
                [':%s' % i for i in range(num)]
            )
            keys = [str(i) for i in range(num)]
            if isinstance(args[0], (list, tuple)):
                args = [dict(zip(keys, i)) for i in args]
            else:
                args = dict(zip(keys, args))
        return self.session.execute(sql, args)

    def query(self, sql, args=None):
        """
        查询返回元祖数据集(clob对象转对应字符串)
        """
        rs = self.execute(sql).fetchall()
        return rs

    def query_dict(self, sql, args=None):
        """
        查询返回字典数据集
        """
        rs = self.execute(sql)
        colunms = [i[0] for i in rs._cursor_description()]
        return [OrderedDict(zip(colunms, i)) for i in rs.fetchall()]

    def insert(self, sql, args, num=10000):
        """
        批量更新插入，默认超过10000条数据自动commit
        insertone:
            self.insert(
                "insert into test(id) values(:id)",
                {'id': 6666}
            )
        insertmany:
            self.insert(
                "insert into test(id) values(:id)",
                [{'id': 6666}, {'id': 8888}]
            )
        @num:
            批量插入时定义一次插入的数量，默认10000
        """
        try:
            length = len(args)
            count = 0
            if args and not isinstance(args, dict)\
                    and isinstance(args[0], dict):
                for i in range(0, length, num):
                    rs = self.execute(sql, args[i:i + num])
                    count += rs.rowcount
            else:
                rs = self.execute(sql, args)
                count = rs.rowcount
        except DatabaseError as reason:
            self.rollback()
            # match_name = re.compile('(?:into|update) +(\S+)')
            # table_name = match_name.findall(sql)[0]
            # cx_Oracle.DatabaseError: ORA-12899:
            # 列 "PPS1DBA"."PATROL_WPXX"."OBJ_NAME" 的值太大 (实际值: 40, 最大值: 25)
            if '的值太大' in str(reason):
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
                self.session.execute(sql_modify)
                count += self.insert(sql, args, num)
            else:
                if args and not isinstance(args, dict)\
                        and isinstance(args[0], (tuple, list, dict)):
                    if num <= 10 or length <= 10:
                        err_msg = ['\nSQL EXECUTEMANY ERROR\n %s' % sql]
                        # for record in args[i:i + num]:
                        #     err_msg.append('\n %s' % record)
                        err_msg.append(json.dumps(args[i:i + num], indent=1))
                        err_msg.append('\nSQL EXECUTEMANY ERROR\n')
                        log.error(''.join(err_msg))
                        raise reason
                    else:
                        self.insert(
                            sql, args[i:i + num],
                            num=reduce_num(num, length))
                else:
                    log.error(
                        '\nSQL EXECUTE ERROR\n%s\n%s\nSQL EXECUTE ERROR\n' %
                        (sql, args)
                    )
                    raise reason
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
        print(sql)
        self.execute(sql, args)
        # length = len(args)
        # for i in range(0, length, num):
        #     self.execute(sql, args[i:i + num])

    def delete_repeat(self, table, unique, cp_field="rowid"):
        """
        数据去重
        默认通过rowid方式去重
        """
        sql = "delete from {table} where {cp_field} is null".format(
            table=table, cp_field=cp_field)
        null_count = self.execute(sql).rowcount
        print(
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
        print('删除重复数据：%s' % count)
        return count

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
