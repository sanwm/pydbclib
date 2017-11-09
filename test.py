from py_db import connection
db = connection('jwdn/password@local:1521/xe', driver="cx_Oracle", debug=True)
# sqlalchemy module
# db = connection("oracle://jwdn:password@local:1521/xe", debug=True)
# db = connection("mysql+pyodbc://:@mysqldb", debug=True)
# base dbapi module
# db = connection('DSN=mysqldb', driver="pyodbc") # 'DSN=mydb;UID=root;PWD=password'
db = connection('DSN=oracledb;PWD=password', driver="pyodbc", debug=True)


def basic():
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
            'id')
        sql = "select id,foo,bar from py_db_test"
        rs2 = db.query(sql)
        print(rs2)
    finally:
        db.execute('drop table py_db_test')

    print('SUCCESS')


if __name__ == '__main__':
    basic()
