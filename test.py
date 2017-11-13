from py_db import connection


def basic(db, db_type):
    db.execute("create table py_db_test(id varchar(20) primary key,foo varchar(100), bar varchar(100))")
    try:
        db.insert("insert into py_db_test(id,foo,bar) values('1','hello','中国')")
        sql = "select foo,bar from py_db_test where id=:1"
        rs1 = db.query(sql, ['1'])
        print(rs1)
        db.insert(
            "insert into py_db_test(id,foo,bar) values(:1,:1,:1)",
            ['2', 'abc', '一二三'])
        db.insert(
            "insert into py_db_test(id,foo,bar) values(:1,:1,:1)",
            [('11', 'a', '一'), ('22', 'b', '二')])
        db.merge(
            'py_db_test',
            [{'id': '11', 'foo': 'a', 'bar': '1'}, {'id': '33', 'foo':'b', 'bar':'二'}],
            'id', db_type=db_type)
        sql = "select id,foo,bar from py_db_test"
        rs2 = db.query(sql)
        print(rs2)
    finally:
        db.execute('drop table py_db_test')

    print('SUCCESS')


def sqlalchemy_test():
    db = connection("oracle://jwdn:password@local:1521/xe", debug=True)
    basic(db, 'oracle')
    db = connection("mysql+pyodbc://:@mysqldb", debug=True)
    basic(db, 'mysql')
    db = connection("mysql+pymysql://root:password@192.168.152.200:3306/awesome?charset=utf8", debug=True)
    basic(db, 'mysql')


def base_test():
    db = connection('jwdn/password@local:1521/xe', driver="cx_Oracle", debug=True)
    basic(db, 'oracle')
    db = connection('DSN=oracledb;PWD=password', driver="pyodbc", debug=True)
    basic(db, 'oracle')
    db = connection('DSN=mysqldb', driver="pyodbc") # 'DSN=mydb;UID=root;PWD=password'
    basic(db, 'mysql')


def main():
    base_test()
    sqlalchemy_test()


if __name__ == '__main__':
    main()
