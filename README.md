## pydbclib: Python Database Connectivity Lib
pydbclib is a database utils for python

### Installation:
    pip install pydbclib

#### usage:

    from pydbclib import connection
    with connection("test.db", driver="sqlite3") as db:
        db.ddl('create table test (id varchar(4) primary key, name varchar(10))')
        count1 = db.write("insert into test (id, name) values('0001', 'lyt')")
        count2 = db.write_by_dict('test', {'id': 2, 'name': 'test2'})
        print("插入的行数:", count1, count2)
        data1 = db.read("select id, name from test")
        data2 = db.read_dict("select id, name from test")
        print("查询到的结果:", data1, data2)
        db.ddl('drop table test')

    # Common Driver
    # 连接oracle
    db = connection('jwdn/password@local:1521/xe', driver="cx_Oracle")
    # 通过odbc方式连接
    db = connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")

    # Sqlalchemy Driver
    # 连接oracle
    db = connection("oracle://jwdn:password@local:1521/xe")
    # 连接mysql
    db = connection("mysql+pyodbc://:@mysqldb")

    # udf扩展
    from pydbclib import Connection  
    class MyUDF(Connection):
        def total_data(self, table):  
            return self.read("select count(*) from :1", table)

    with MyUDF("oracle://lyt:lyt@local:1521/xe") as db:
        count = db.total_data('test')
        print("test表的总数量为:", count)
