# Avro_kafka_example
Example of using kafka connector, avro, and mysql sink

Command
1. python avro_producer.py -b "localhost:9092" -t "BTCUSDT" -s "http://localhost:8081"
2. curl -d @"sinkMysql_btc.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors

# Example of querying data from the mysql running inside a docker
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
