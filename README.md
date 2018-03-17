## py_db: database for humans
py_db is a database utils for python

### Installation:
    pip install py_db

#### usage:

    from py_db import connection
    with connection("test.db", driver="sqlite3") as db:
        db.execute('create table test (id varchar(4) primary key, name varchar(10))')
        count1 = db.insert("insert into test (id, name) values('0001', 'lyt')")
        count2 = db.insert_by_dict('test', {'id': 2, 'name': 'test2'})
        print("插入的行数:", count1, count2)
        data = db.query("select id, name from test")
        print("查询到的结果:", data)
        db.execute('drop table test')

    # base dbapi module
    # 连接oracle
    db = connection('jwdn/password@local:1521/xe', driver="cx_Oracle", debug=True)
    # 通过odbc方式连接
    db = connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")

    # sqlalchemy module
    # 连接oracle
    db = connection("oracle://jwdn:password@local:1521/xe", debug=True)
    # 连接mysql
    db = connection("mysql+pyodbc://:@mysqldb", debug=True)
