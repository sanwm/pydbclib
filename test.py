from py_db import connection

# db = connection("oracle://jwdn:password@local:1521/xe", debug=True)
# db = connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")
db = connection('jwdn/password@local:1521/xe', driver="cx_Oracle", debug=True)
sql = "select mjjh,cjsj from accident where rownum <20"
sql = "select mjjh,mjbm from accident where rownum <20"
sql = "select id,name from test where rownum <20"
res = db.query_dict(sql)
db.insert("insert into test1 values(:id,:name)", res)
db.commit()
print('end')
