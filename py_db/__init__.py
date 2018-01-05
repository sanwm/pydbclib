# -*- coding: utf-8 -*-

__author__ = "lyt"

"""
    db connection
"""
__version__ = '1.0.0'

__all__ = ['connection']

from py_db.abstract import DbapiFactory
from py_db import base
from py_db import sqlalchemy
# from py_db.utils import run_time


def _connection(*args, **kwargs):
    """
    sqlalchemy uri:
        connection("oracle+cx_oracle://jwdn:password@local:1521/xe")
    common db uri:
        connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")
        connection(
            host='172.16.17.18', port=21050,
            use_kerberos=True, kerberos_service_name="impala",
            timeout=3600, driver="impala.dbapi")
    """
    # if kwargs.get('debug'):
    #     log.setLevel(logging.DEBUG)
    # kwargs.pop('debug', None)
    driver = kwargs.get('driver')
    if driver is None:
        # from . import sqlalchemy
        return sqlalchemy.Connection(*args, **kwargs)
    else:
        # from . import base
        return base.Connection(*args, **kwargs)


# @run_time
def connection(*args, **kw):
    kw = kw.copy()
    rs = kw.pop('uri', None)
    args = (rs,) if rs else () + args
    # print(args, kw)
    db = _connection(*args, **kw)
    return DbapiFactory(db)
