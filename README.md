# Avro_kafka_example
Example of using kafka connector, avro, and mysql sink

Command
1. python avro_producer.py -b "localhost:9092" -t "BTCUSDT" -s "http://localhost:8081"
2. curl -d @"sinkMysql_btc.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors

# Example of querying data from the mysql running inside a docker

## pip install mysqlclient

```python
import MySQLdb

host = '127.0.0.1'
user = 'root'
password = 'confluent'
port = 3306
db = 'connect_test'

conn = MySQLdb.Connection(
    host=host,
    user=user,
    passwd=password,
    port=port,
    db=db
)

# Example of how to fetch table data:
conn.query("""SELECT * FROM BTCUSDT""")
result = conn.store_result()
for i in range(result.num_rows()):
    print(result.fetch_row())
```
# To save data as a csv file
```python
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
```
