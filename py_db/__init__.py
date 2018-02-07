# -*- coding: utf-8 -*-
"""
    db connection
"""
__author__ = "lyt"
__version__ = '1.1rc0'
__all__ = ['connection']

from py_db.adapter import ConAdapter
from py_db import base
from py_db import sqlalchemy


def _connection_factory(*args, **kwargs):
    """
    工厂模式
    """
    driver = kwargs.get('driver')
    if driver is None:
        return sqlalchemy.Connection(*args, **kwargs)
    else:
        return base.Connection(*args, **kwargs)


def connection(*args, **kw):
    """
    适配器模式
    sqlalchemy mode:
        >>> db = connection("oracle://jwdn:password@local:1521/xe")
    common adapter mode:
        >>> db = connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")
        >>> db = connection(
            host='172.16.17.18', port=21050,
            use_kerberos=True, kerberos_service_name="impala",
            timeout=3600, driver="impala.dbapi")
    usage:
        >>> db.query("select id,name from py_db_test where name=:1", ['test'])
        []
        >>> db.insert("insert into py_db_test(id,name) values(:id, :name)", {'id': 2, 'name': 'test'})
        1
        >>> db.query("select id,name from py_db_test where name=:name", {'name': 'test'})
        [(1, test)]
        >>> db.dict_query("select id,name from py_db_test where name=:name", {'name': 'test'})
        [{'id':1, 'name': 'test'}]
    """
    kw = kw.copy()
    uri = kw.pop('uri', None)
    dsn = kw.pop('dsn', None)
    rs = uri or dsn
    args = (rs,) if rs else () + args
    return ConAdapter(_connection_factory(*args, **kw))
