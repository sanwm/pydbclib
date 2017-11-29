import sys


class DbapiFactory(object):
    def __init__(self, db):
        self.db = db
        self.connect = db.connect
        self.session = db.session
        self.log = db.log

    def execute(self, sql, args=[], num=10000):
        return self.db.execute(sql, args, num)

    def insert(self, sql, args=[], num=10000):
        return self.db.insert(sql, args, num)

    def query(self, sql, args=[], size=None):
        return self.db.query(sql, args, size)

    def query_dict(self, sql, args=[], ordered=False, size=None):
        return self.db.query_dict(sql, args, ordered, size)

    def merge(self, table, args, unique, num=10000, db_type=None):
        check = (not args or not isinstance(args, (tuple, list)) or
                 not isinstance(args[0], dict))
        if check:
            self.log.error("args 形式错误，必须是字典集合 "
                           "for example([{'a':1},{'b':2}])")
            sys.exit(1)

        db = db_type.lower() if isinstance(db_type, str) else None
        columns = [i for i in args[0].keys()]
        if (set(unique) & set(columns)) != set(unique):
            self.log.error("columns(%s) 中没有 unique(%s)" % (columns, unique))
            sys.exit(1)
        if db == "oracle":
            self.oracle_merge(table, args, columns, unique, num)
        elif db == "mysql":
            self.mysql_merge(table, args, columns, unique, num)
        elif db == "postgressql":
            self.postgressql_merge(table, args, columns, unique, num)
        else:
            self.common_merge(table, args, columns, unique, num)

    def oracle_merge(self, table, args, columns, unique, num=10000):
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
               " INSERT ({t1_columns}) VALUES ({t2_columns})".format(
                   table=table, param_columns=param_columns,
                   unique=unique, update_field=update_field,
                   t1_columns=t1_columns, t2_columns=t2_columns))
        self.db.insert(sql, args)

    def mysql_merge(self, table, args, columns, unique, num=10000):
        values = ','.join([':%s' % i for i in columns])
        field = ','.join(["{0}={0}".format(i) for i in columns if i != unique])
        sql = ("INSERT INTO {table}({columns}) VALUES({values}) "
               "ON DUPLICATE KEY UPDATE {update_field}".format(
                   table=table, columns=','.join(columns),
                   values=values, update_field=field))
        self.db.insert(sql, args)

    def postgressql_merge(self, table, args, columns, unique, num=10000):
        values = ','.join([':%s' % i for i in columns])
        field = ','.join(["{0}={0}".format(i) for i in columns if i != unique])
        sql = ("INSERT INTO {table}({columns}) "
               "VALUES({values}) "
               "ON conflict({unique})"
               "DO UPDATE SET {update_field}".format(
                   table=table, unique=unique, columns=','.join(columns),
                   values=values, update_field=field))
        self.db.insert(sql, args)

    def common_merge(self, table, args, columns, unique, num=10000):
        values = ','.join([':%s' % i for i in columns])
        if isinstance(unique, (list, tuple)):
            unique = ' and '.join(['{0}=:{0}'.format(i) for i in unique])
            sql_del = "delete from {0} where {1}".format(table, unique)
        else:
            sql_del = "delete from {0} where {1}=:{1}".format(table, unique)
        sql_in = "INSERT INTO {table}({columns}) VALUES({values})".format(
            table=table, columns=','.join(columns), values=values)
        self.db.insert(sql_del, args)
        self.db.insert(sql_in, args)

    def delete_repeat(self, table, unique, cp_field="rowid"):
        """
        oracle 数据去重, 默认通过rowid方式去重
        """
        sql = "delete from {table} where {cp_field} is null".format(
            table=table, cp_field=cp_field)
        null_count = self.db.execute(sql).rowcount
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
        rs = self.db.execute(sql)
        count = rs.rowcount
        log.info('删除重复数据：%s' % count)
        return count

    def empty(self, table):
        sql = "truncate table %s" % table
        self.db.insert(sql)

    def rollback(self):
        self.db.rollback()

    def commit(self):
        self.db.commit()

    def close(self):
        self.db.close()

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
