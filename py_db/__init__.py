# -*- coding: utf-8 -*-

__author__ = "lyt"

"""
    db connection
"""
__version__ = '0.0.8'

__all__ = ['connection']

import logging
from py_db.logger import log


def connection(*args, **kwargs):
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
    if kwargs.get('debug'):
        log.setLevel(logging.DEBUG)
    kwargs.pop('debug', None)
    driver = kwargs.get('driver')
    if driver is None:
        from . import sqlalchemy
        return sqlalchemy.Connection(*args, **kwargs)
    else:
        from . import base
        return base.Connection(*args, **kwargs)
