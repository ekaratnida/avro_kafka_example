import MySQLdb as dbapi
import sys
import csv

QUERY='SELECT * FROM connect_test.BTCUSDT;'
db=dbapi.connect(host='127.0.0.1',user='root',passwd='confluent')

cur=db.cursor()
cur.execute(QUERY)
result=cur.fetchall()
	
with open("btc_usdt.csv", "w") as f:
    csv_writer = csv.writer(f)
    for mytuple in result:
        csv_writer.writerow(mytuple)