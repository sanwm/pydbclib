# -*- coding: utf-8 -*-
"""
    db connection
"""
__version__ = '0.0.3'

__all__ = ['connection']

# from sqlalchemy import engine


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
    driver = kwargs.get('driver')
    if driver is None:
        from . import sqlalchemy_db
        return sqlalchemy_db.Connection(*args)
    else:
        from . import base
        return base.Connection(*args, **kwargs)
