#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of AvroSerializer.
from confluent_kafka.admin import AdminClient, NewTopic
import argparse
import os
import requests

from time import sleep

from uuid import uuid4

from six.moves import input

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class User(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color

        address(str): User's address; confidential
        {
          "symbol": "BTCUSDT",
          "openPrice": "61729.27000000",
          "highPrice": "61800.00000000",
          "lowPrice": "61319.47000000",
          "lastPrice": "61699.01000000",
          "volume": "814.22297000",
          "quoteVolume": "50138059.82771860",
          "openTime": 1715732880000,
          "closeTime": 1715736489761,
          "firstId": 3599114332,
          "lastId": 3599147596,
          "count": 33265
        }
    """

    def __init__(self, symbol, openPrice, highPrice, lowPrice, lastPrice, volume, openTime, closeTime, count):
        self.symbol = symbol
        self.openPrice = openPrice
        self.highPrice = highPrice
        self.lowPrice = lowPrice
        self.lastPrice = lastPrice
        self.volume = volume
        self.openTime = openTime
        self.closeTime = closeTime
        self.count = count


def user_to_dict(user, ctx):
    
    # User._address must not be serialized; omit from dict
    return dict(symbol=user.symbol, openPrice=user.openPrice, highPrice=user.highPrice, lowPrice=user.lowPrice,
                lastPrice=user.lastPrice, volume=user.volume, openTime=user.openTime, closeTime=user.closeTime,
                count=user.count)


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic
    is_specific = args.specific == "true"

    if is_specific:
        schema = "user_specific.avsc"
    else:
        schema = "user_generic.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     user_to_dict)

    string_serializer = StringSerializer('utf_8')

    producer_conf = {'bootstrap.servers': args.bootstrap_servers}

    producer = Producer(producer_conf)

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    
    coin = topic
    api = "https://api.binance.com/api/v3/ticker?type=MINI&symbol="+coin
    
    #Create topic with a number of partition and replicas
    admin_client = AdminClient(producer_conf)
    topic_list = []
    topic_list.append(NewTopic(topic, 2, 3))
    admin_client.create_topics(topic_list)

  
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        
        data = requests.get(api).json()
        print(data)

        user = User(symbol=str(data["symbol"]), openPrice=float(data["openPrice"]), highPrice= float(data["highPrice"]),
                    lowPrice=float(data["lowPrice"]),lastPrice=float(data["lastPrice"]), volume=float(data["volume"]), 
                    openTime=int(data["openTime"]),closeTime= int(data["closeTime"]), count=int(data["count"]))
            
        producer.produce(topic=topic,
                             key=string_serializer(topic),
                             value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
        
        sleep(3)
        producer.poll(0.0)
        
    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroSerializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())

#Example
# python avro_producer.py -b "localhost:9092" -t "BTCUSDT" -s "http://localhost:8081"