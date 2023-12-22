import sqlite3 as mdb
def chk_conn(conn):
     try:
        conn.cursor()
        return True
     except Exception as ex:
        return False

myconn = mdb.connect('spotify.db')
print(chk_conn(myconn))
myconn.close()