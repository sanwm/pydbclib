from py_db import connection
db = connection('jwdn/password@local:1521/xe', driver="cx_Oracle", debug=True)
# sqlalchemy module
db = connection("oracle://jwdn:password@local:1521/xe", debug=True)
# db = connection("mysql+pyodbc://:@mysqldb", debug=True)
# base dbapi module
# db = connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")
# db = connection('DSN=oracledb;PWD=password', driver="pyodbc", debug=True)


def basic():
    db.execute("create table py_db_test(foo varchar(100), bar varchar(100))")
    try:
        db.insert("insert into py_db_test(foo,bar) values('hello','中国')")
        sql = "select foo,bar from py_db_test"
        rs1 = db.query(sql)
        sql = "select foo,bar,:1 from py_db_test"
        rs2 = db.query(sql, ['test'])
        print(rs1)
        print(rs2)
        db.insert(
            "insert into py_db_test(foo,bar) values(:1,:1)",
            ['abc', '一二三'])
        db.insert(
            "insert into py_db_test(foo,bar) values(:1,:1)",
            [('a', '一'), ('b', '二'), ('c', '三')])
    finally:
        db.execute('drop table py_db_test')

    print('SUCCESS')


if __name__ == '__main__':
    basic()
