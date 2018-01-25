## a utils used to operate database and can match the common database

#### usage:

    from py_db import connection
    # base dbapi module
    db = connection('jwdn/password@local:1521/xe', driver="cx_Oracle", debug=True)
    db = connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")

    # sqlalchemy module
    db = connection("oracle://jwdn:password@local:1521/xe", debug=True)
    db = connection("mysql+pyodbc://:@mysqldb", debug=True)