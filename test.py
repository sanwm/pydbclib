from py_db import connection
# sqlalchemy module
# db = connection("oracle://jwdn:password@local:1521/xe", debug=True)
# db = connection("mysql+pyodbc://:@mysqldb", debug=True)
# base dbapi module
# db = connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")
db = connection(con='DSN=oracledb;PWD=password', driver="pyodbc", debug=True)
# db = connection('jwdn/password@local:1521/xe', driver="cx_Oracle", debug=True)


sql = "select mjjh,sgddms from accident where rownum <?"
res = db.query(sql, [10])
print(res)


# db = connection("mysql+pyodbc://:@mysqldb", debug=True)
# db = connection('DSN=mysqldb', driver="pyodbc")
# db = connection('DSN=oracledb;PWD=password', driver="pyodbc")


# db.insert("insert into py_db(foo,bar) values(:1,:1)", res)
# db.commit()
print('end')
