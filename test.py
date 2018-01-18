from py_db import connection
debug = True
debug = False


def basic(db, db_type=None):
    db.execute("create table py_db_test(id varchar(20) primary key,foo varchar(100), bar varchar(100))")
    try:
        db.insert("insert into py_db_test(id,foo,bar) values('aaa','hello','中国')")
        sql1 = "select id,foo,bar from py_db_test where id=:a and foo=:a"
        sql = "select id,foo,bar from py_db_test"
        rs1 = db.query(sql1, {'a': '1', 'b': '2'})
        print(rs1)
        db.insert(
            "insert into py_db_test(id,foo,bar) values(:1,:1,:1)",
            ['bbb', 'abc', '一二三'])
        db.insert(
            "insert into py_db_test(id,foo,bar) values(:1,:1,:1)",
            [('1', 'a', '一'), ('2', 'b', '二')])
        print(db.query(sql))
        param = [{'id': '1', 'foo': 'a', 'bar': 'one'},
                 {'id': '2', 'foo': 'b', 'bar': 'two'},
                 {'id': '3', 'foo': 'c', 'bar': 'three'}]
        db.merge('py_db_test', param, 'id', db_type=db_type)
        print(db.query(sql))
    finally:
        db.execute('drop table py_db_test')


def sqlalchemy_test():
    # debug = False
    db = connection("oracle://jwdn:jwdn@local:1521/xe", debug=debug)
    basic(db, 'oracle')
    db = connection("mysql+pyodbc://:@mysqldb", debug=debug)
    basic(db, 'mysql')
    db1 = connection(
        "mysql+pymysql://root:hadoop@centos:3306/test?charset=utf8",
        debug=debug)
    basic(db1, 'mysql')
    kw = {
        # "echo": True,
        "uri": 'mysql+pymysql://root:hadoop@centos:3306/test?charset=utf8',
        'debug': debug
    }
    db2 = connection(**kw)
    basic(db2)
    basic(db1)


def base_test():
    db = connection(
        'jwdn/jwdn@local:1521/xe',
        driver="cx_Oracle", debug=debug)
    basic(db, 'oracle')
    db = connection('DSN=oracledb;PWD=jwdn', driver="pyodbc", debug=debug)
    basic(db, 'oracle')
    # 'DSN=mydb;UID=root;PWD=password'
    db = connection(dsn='DSN=mysqldb', driver="pyodbc")
    basic(db, 'mysql')
    db = connection(
        host='centos',
        user='root',
        password='password',
        database='test',
        charset='utf8',
        driver="pymysql",
        debug=debug,
        placeholder='%s')
    basic(db, 'mysql')
    kw = {
        # "uri": 'jwdn/jwdn@local:1521/xe',
        'driver': "cx_Oracle",
        # 'debug': debug
    }
    db = connection(uri='jwdn/jwdn@local:1521/xe', **kw)
    basic(db)
    basic(db, "oracle")


def basic_single():
    db = connection(uri='DSN=mysqldb', driver="pyodbc", debug=debug)
    db.execute("create table py_db_test(id varchar(20) primary key,foo varchar(100), bar varchar(100))")
    try:
        db.insert("insert into py_db_test(id,foo,bar) values('aaa','hello','中国')")
        print(db.query("select * from py_db_test where id=:1", ['aaa']))
        print(db.query("select * from py_db_test where id=:1", ['1']))
        print(db.query("select max(foo) from py_db_test where id=:1", ['1']))
    finally:
        db.execute('drop table py_db_test')


def sqlalchemy_single():
    db = connection(dsn="oracle://jwdn:jwdn@local:1521/xe", debug=debug)
    print(db.connect)
    db.execute("create table py_db_test(id varchar(20) primary key,foo varchar(100), bar varchar(100))")
    try:
        db.insert("insert into py_db_test(id,foo,bar) values('aaa','hello','中国')")
        print(db.query("select * from py_db_test where id=:1", ['aaa']))
        print(db.query("select * from py_db_test where id=:1", ['1']))
        print(db.query("select max(id) from py_db_test where id=:1", ['1']))
    finally:
        db.execute('drop table py_db_test')
        print(db.connect)
    print(db.connect)


def test():
    # with connection(host='localhost', port=10000, user='hive', password='hive', database='default', auth_mechanism='PLAIN', driver="impala.dbapi") as db:
    db1 = connection("oracle://jwdn:jwdn@local:1521/xe", echo=True)
    db1.query("select * from SS1")
    db2 = connection("oracle://jwdn:jwdn@local:1521/xe", echo='debug')
    db2.query("select * from SS1")
    db1.query("select * from SS1")


def main():
    # base_test()
    # sqlalchemy_test()
    # basic_single()
    sqlalchemy_single()
    print('SUCCESS')


if __name__ == '__main__':
    main()
    # test()
