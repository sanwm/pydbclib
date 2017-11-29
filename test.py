from py_db import connection
debug = True


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
    debug = False
    db = connection("oracle://jwdn:password@local:1521/xe", debug=debug)
    basic(db, 'oracle')
    db = connection("mysql+pyodbc://:@mysqldb", debug=debug)
    basic(db, 'mysql')
    db1 = connection(
        "mysql+pymysql://root:password@centos:3306/awesome?charset=utf8",
        debug=debug)
    basic(db1, 'mysql')
    kw = {
        # "echo": True,
        "uri": 'mysql+pymysql://root:password@centos:3306/awesome?charset=utf8',
        'debug': debug
    }
    db2 = connection(**kw)
    basic(db2)
    basic(db1)


def base_test():
    db = connection(
        'jwdn/password@local:1521/xe',
        driver="cx_Oracle", debug=debug)
    basic(db, 'oracle')
    db = connection('DSN=oracledb;PWD=password', driver="pyodbc", debug=debug)
    basic(db, 'oracle')
    # 'DSN=mydb;UID=root;PWD=password'
    db = connection('DSN=mysqldb', driver="pyodbc")
    basic(db, 'mysql')
    db = connection(
        host='centos',
        user='root',
        password='password',
        database='awesome',
        charset='utf8',
        driver="pymysql",
        debug=debug,
        placeholder='%s')
    basic(db, 'mysql')
    kw = {
        # "uri": 'jwdn/password@local:1521/xe',
        'driver': "cx_Oracle",
        # 'debug': debug
    }
    db = connection(uri='jwdn/password@local:1521/xe', **kw)
    basic(db)
    basic(db, "oracle")


def main():
    base_test()
    sqlalchemy_test()
    print('SUCCESS')


if __name__ == '__main__':
    main()
    db = connection('DSN=mysqldb', driver="pyodbc")
    print(db.query("select * from py_etl where id=?", [111]))
