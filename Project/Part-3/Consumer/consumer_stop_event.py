#!/usr/bin/env python
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
#
# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
from confluent_kafka import Consumer
import json
import ccloud_lib
import datetime
import pandas as pd
import time
import psycopg2
import psycopg2.extras
import argparse
import re
import csv
from dateutil import parser
DBname = "postgres"
DBuser = "postgres"
DBpwd = "root123"
# connect to the database

def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = False
    return connection
    
def execute_batch_Trip(conn, tripData, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
    """
    cursor=conn.cursor()
    table = "Trip"
    cursor.execute("PREPARE updateStmt AS UPDATE Trip SET route_id=$1,service_key=$2,direction=$3 where trip_id=$4")
    try:
        psycopg2.extras.execute_batch(cursor,"EXECUTE updateStmt (%(route_id)s, %(service_key)s, %(direction)s, %(trip_id)s)",tripData,page_size=page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    # Create a list of tupples from the dataframe values
    print("execute_batch_Trip done")
    cursor.close()

def validateData(df):
    def calcDir(dir):
        if dir==1:
            return "Back"
        else:
            return "Out"
    def calcService(ser):
        if ser=='S':
            return "Saturday"
        elif ser=="U":
            return "Sunday"
        else:
            return "W"
    df=df.dropna()
    df=pd.DataFrame().assign(trip_id=df["Trip id"],route_id=df["route_number"],service_key=df["service_key"],direction=df["direction"])
    df["direction"]=df["direction"].apply(calcDir)
    df["service_key"]=df["service_key"].apply(calcService)
    return df
    
def load_data(df):
    conn = dbconnect()
    tripData=[]
    for index,row in df.iterrows():
        data={}
        data["trip_id"]=row["trip_id"]
        data["route_id"]=row["route_number"]
        data["service_key"]=row["service_key"]
        data["direction"]=row["direction"]
        tripData.append(data)
    execute_batch_Trip(conn, tripData, 100)
    
if __name__ == '__main__':
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)
    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)
    # Subscribe to topic
    consumer.subscribe([topic])
    # Process messages
    jsonData = []
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                if len(jsonData) > 0:
                   df = pd.DataFrame(jsonData)
                   df = validateData(df)
                   load_data(df)
                #   jsonData.clear()
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                jsonData.append(data)
                print("Consumed record with key {} and value {}, \
                      and data is {}"
                      .format(record_key, record_value, data))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()