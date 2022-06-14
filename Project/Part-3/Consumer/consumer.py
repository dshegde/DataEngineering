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
MI_PER_HOUR_CONVERSION_FACTOR = 2.237

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

def execute_batch_Trip(conn, df, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
    """
    table = "Trip"
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    print(cols)
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_batch(cursor, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_batch_Trip done")
    cursor.close()

def execute_batch_breadCrumb(conn, df, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
    """
    table = "BreadCrumb"
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    print(cols)
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_batch(cursor, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_batch_BreadCrumb done")
    cursor.close()
    
def validateData(df):
    print(df)
    cols = ['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'VEHICLE_ID', 'METERS', 'VELOCITY', 'DIRECTION', 'GPS_LONGITUDE', 'GPS_LATITUDE','GPS_SATELLITES','GPS_HDOP']
    df[cols] = df[cols].apply(lambda x: pd.to_numeric(x, errors='coerce'))

    # limit assertions - The value of DIRECTION column ranges from 0 to 360
    def filter_by_direction(direction):
        return float(direction) >= 0 and float(direction) <= 360

    df = df[df['DIRECTION'].apply(filter_by_direction)]

    # Intra-record assertion - Whenever a gps_longitute is mentioned, the gps_latitude must also be present
    df = df.dropna(subset=['GPS_LONGITUDE', 'GPS_LATITUDE'], how='any')

    # Existence assertion - Every trip must have a “trip no” associated with it.
    df = df.dropna(subset=['EVENT_NO_TRIP'])

    # Inter-record assertion - The vehicle id should remain the same for a particular unique event trip no throughout the trip.
    UniqueTripIds = df['EVENT_NO_TRIP'].drop_duplicates()

    idToBusCountMap = {}
    for index, value in UniqueTripIds.items():
        uniqueVechilesCountPerId = df.loc[df['EVENT_NO_TRIP'] == value    , 'VEHICLE_ID']
        idToBusCountMap[value] = uniqueVechilesCountPerId.drop_duplicates().count()

    violatingTrips = dict(filter(lambda elem: int(elem[1]) > 1,idToBusCountMap.items()))
    violationTripIds = list(violatingTrips.keys())

    df = df[~df['EVENT_NO_TRIP'].isin(violationTripIds)]

    # GPS_HDOP value is always in the range 0 - 20
    def filter_by_hdop(gps_hdop):
        return float(gps_hdop) >= 0 and float(gps_hdop) <= 20

    df = df[df['GPS_HDOP'].apply(filter_by_hdop)]

    # GPS_SATELLITES can never be 0, negative or empty
    def filter_by_gps_satellite_count(satellites):
        return float(satellites) > 0

    df = df[df['GPS_SATELLITES'].apply(filter_by_gps_satellite_count)]

    # GPS_LONGITUDE must always be a negative value (As US is in western hemisphere) and GPS_LATITUDE must always be a positive value (As we are in northern hemisphere)
    def filter_by_GPS_LONGITUDE(longitude):
        return float(longitude) < 0

    df = df[df['GPS_LONGITUDE'].apply(filter_by_GPS_LONGITUDE)]

    def filter_by_GPS_LATITUDE(latitude):
        return float(latitude) > 0

    df = df[df['GPS_LATITUDE'].apply(filter_by_GPS_LATITUDE)]

    #LIMIT ASSERTION  METERS must always be greater than equal to zero

    def filter_by_meters(mtr):
	    return float(mtr) >= 0

    df = df.dropna(subset=['METERS'],how='any')


    df['METERS'] = pd.to_numeric(df['METERS'])

    df = df[df['METERS'].apply(filter_by_meters)]


    #INTER-RECORD ASSERTION EVENT_NO_TRIP VALUE SHOULD ALWAYS BE LESSER THAN EVENT_NO_STOP

    def filter_by_event_no_trip(trp):
      return float(trp)>=0

    def filter_by_event_no_stop(trp):
      return float(trp)>=0

    def validTrip(event):
      return event['EVENT_NO_TRIP']<=event['EVENT_NO_STOP']

    df=df.dropna(subset=['EVENT_NO_TRIP','EVENT_NO_STOP'],how='any')

    df['EVENT_NO_TRIP']=pd.to_numeric(df['EVENT_NO_TRIP'])

    df['EVENT_NO_STOP']=pd.to_numeric(df['EVENT_NO_STOP'])


    df=df[df['EVENT_NO_TRIP'].apply(filter_by_event_no_trip)]

    df=df[df['EVENT_NO_STOP'].apply(filter_by_event_no_stop)]

    df=df[df.apply(lambda row:validTrip(row),axis=1)]

    return df

def load_data(df):

    def populateDirection(row):
        direction = float(row['DIRECTION'])
        if direction > 0:
            return 'Out'
        else:
            return 'Back'

    def populateServiceKey(row):
        dt = parser.parse(row['OPD_DATE'])
        day_number = dt.weekday()
        if day_number == 5:
            day = 'Saturday'
        elif day_number == 6:
            day = 'Sunday'
        else:
            day = 'Weekday'
        return day

    def getTimestamp(row):
        dt = parser.parse(row['OPD_DATE'])
        fulldate = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
        fulldate = fulldate + datetime.timedelta(seconds=int(row['ACT_TIME']))
        return fulldate

    def getSpeed(row):
        velocity = float(row['VELOCITY'])
        return velocity * MI_PER_HOUR_CONVERSION_FACTOR

    tripTableDf = df.drop(['EVENT_NO_STOP', 'METERS', 'ACT_TIME', 'RADIO_QUALITY', 'VELOCITY', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP', 'SCHEDULE_DEVIATION'], axis = 1)    
    tripTableDf = tripTableDf.rename(columns={"EVENT_NO_TRIP": "TRIP_ID"})
    tripTableDf = tripTableDf.drop_duplicates(subset='TRIP_ID')
    tripTableDf['service_key'] = tripTableDf.apply(lambda row: populateServiceKey(row), axis=1)
    tripTableDf = tripTableDf.drop(['OPD_DATE'], axis = 1)
    tripTableDf['DIRECTION'] = tripTableDf.apply(lambda row: populateDirection(row), axis=1)
    tripTableDf['route_id'] = 0

    print(tripTableDf)

    breadCrumbDf = df.drop(['EVENT_NO_STOP', 'VEHICLE_ID', 'METERS', 'RADIO_QUALITY', 'GPS_SATELLITES', 'GPS_HDOP', 'SCHEDULE_DEVIATION'], axis = 1)
    breadCrumbDf = breadCrumbDf.rename(columns={"EVENT_NO_TRIP": "TRIP_ID", "GPS_LONGITUDE": "longitude", "GPS_LATITUDE": "latitude"})
    breadCrumbDf['ACT_TIME'] = breadCrumbDf.apply(lambda row: getTimestamp(row), axis=1)
    breadCrumbDf = breadCrumbDf.drop(['OPD_DATE'], axis = 1)
    breadCrumbDf['VELOCITY'] = breadCrumbDf.apply(lambda row: getSpeed(row), axis=1)
    breadCrumbDf = breadCrumbDf.rename(columns={'ACT_TIME':'tstamp', "VELOCITY":"speed"})
    print(breadCrumbDf)

    conn = dbconnect()
    execute_batch_Trip(conn, tripTableDf)
    execute_batch_breadCrumb(conn, breadCrumbDf)

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
                   jsonData.clear()
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

