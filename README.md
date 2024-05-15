# avro_kafka_example
Example of using kafka connector, avro, and mysql sink

Command
1. python avro_producer.py -b "localhost:9092" -t "BTCUSDT" -s "http://localhost:8081"
2. curl -d @"sinkMysql_btc.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
