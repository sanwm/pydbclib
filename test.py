from py_db import connection

# db = connection("oracle+cx_oracle://jwdn:password@local:1521/xe")
# db = connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")
db = connection('jwdn/password@local:1521/xe', driver="cx_Oracle")
res = db.query("select mjjh,cjsj from accident where rownum <20")
# res = db.query("select mjjh,mjbm from accident where rownum <20")
# print(res)
db.insert("insert into test values(:1,:1)", res)
db.commit()
print('end')
