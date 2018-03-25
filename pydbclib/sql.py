# -*- coding: utf-8 -*-
"""
sql 语句参数名称提取及参数形式替换

"""
import re


class Parse(object):

    def __init__(self, sql, repl_symbol):
        self.sql = sql
        self._repl_symbol = repl_symbol
        self.param_names = []
        self._pattern = re.compile(r":(?P<value>\w+)")
        self._init_sql()

    def _gen_replace_func(self):
        def wrapper1(matched):
            nonlocal i
            i = i + 1
            param_name = "p%s" % i
            self.param_names.append(param_name)
            return ":%s" % param_name
        def wrapper2(matched):
            param_name = matched.group("value")
            self.param_names.append(param_name)
            return self._repl_symbol
        i = 0
        if self._repl_symbol is None:
            return wrapper1
        else:
            return wrapper2

    def _init_sql(self):
        self.sql = self._pattern.sub(self._gen_replace_func(), self.sql)


def handle(sql, repl_symbol=None):
    """
    :param sql: sql语句
    :param repl_symbol: sql中参数替换符号
    :return: 被转化参数格式的sql 和 从原sql中解析出的参数名列表

    >>> handle("select :123,:321,:123 from dual where a=:a")
    ('select :p1,:p2,:p3 from dual where a=:p4', ['p1', 'p2', 'p3', 'p4'])
    >>> handle("select :123,:321,:123 from dual where a=:a", '?')
    ('select ?,?,? from dual where a=?', ['123', '321', '123', 'a'])
    """
    parser = Parse(sql, repl_symbol)
    sql = parser.sql
    keys = parser.param_names
    return sql, keys


if __name__ == "__main__":
    sql, param_names = handle("select :123,:321,:123 from dual where a=:a")
    print(sql, param_names)
    sql, param_names = handle("select :123,:321,:123 from dual where a=:a", '?')
    print(sql.__repr__(), param_names.__repr__())
