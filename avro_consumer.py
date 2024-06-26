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


# A simple example demonstrating use of AvroDeserializer.

import argparse
import os

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from time import sleep
import pandas as pd
from pycaret.regression import load_model, predict_model
from sklearn.metrics import mean_squared_error

from river import linear_model
from river import tree
#model = linear_model.LinearRegression()
model = tree.HoeffdingTreeRegressor(
        grace_period=100,
        model_selector_decay=0.9
    )

class User(object):
    """
    User record

    Args:
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


def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None

    #return User(name=obj['name'],
    #            favorite_number=obj['favorite_number'],
    #            favorite_color=obj['favorite_color'])

    return User(symbol=obj['symbol'], openPrice=obj['openPrice'], highPrice=obj['highPrice'], 
                lowPrice=obj['lowPrice'],lastPrice=obj['lastPrice'], volume=obj['volume'], 
                openTime=obj['openTime'], closeTime=obj['closeTime'],count=obj['count'])



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

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_user)

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'group.id': args.group,
                     'auto.offset.reset': "latest"}

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            '''if user is not None:
                print("User record {}: symbol: {}"
                      "\topenPrice: {}"
                      "\thighPrice: {}"
                      "\tlowPrice: {}"
                      "\tvolume: {}"
                      "\topenTime: {}"
                      "\tcloseTime: {}"
                      "\tcount: {}"
                      "\tlastPrice: {}\n"
                      .format(msg.key(), 
                              user.symbol,
                              user.openPrice,
                              user.highPrice, 
                              user.lowPrice, 
                              user.volume, 
                              user.openTime, 
                              user.closeTime, 
                              user.count,
                              user.lastPrice)
                    )'''
                
            # Create a dataframe for the consuming data to feed into the ML model.
            # For example
            
            data = {'volume':user.volume, 'lowPrice':user.lowPrice,
                    'highPrice':user.highPrice,'count':user.count,
                    'openPrice':user.openPrice}
            
            df = pd.DataFrame(data,index=[user.closeTime])
          
            saved_lr = load_model('model_et')
            predictions = predict_model(saved_lr, data=df)
            
            #print(type(predictions))
            print("Predicted", predictions.iloc[0]['prediction_label']," VS Actual=",user.lastPrice)
            print(mean_squared_error([user.lastPrice] , [predictions.iloc[0]['prediction_label']] ) )

            y_pred = model.predict_one(data)
            model.learn_one(data, user.lastPrice)
            print("y_pred = ",y_pred)



        except KeyboardInterrupt:
            break

        sleep(3)

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())


# Example
# python avro_consumer.py -b "localhost:9092" -s "http://localhost:8081" -t "BTCUSDT" -g "btc"