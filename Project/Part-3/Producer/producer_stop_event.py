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
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import os
import requests
import re
from bs4 import BeautifulSoup
import pandas as pd


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            #print("Produced record to topic {} partition [{}] @ offset {}"
                  #.format(msg.topic(), msg.partition(), msg.offset()))
    #count = 0
    url = "http://www.psudataeng.com:8000/getStopEvents/"
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, "html.parser")
    
    
    # Get all trip ids
    def getTripNumberRegex(str):
        return re.findall(r'\d+', str)
    
    trip_ids = []
    heading_tags = ["h3"]
    for tags in soup.find_all(heading_tags):
        trip_ids.append(getTripNumberRegex(tags.text.strip())[0])
    
    c_tran_tables = soup.find_all("table")
    c_tran_table_data = c_tran_tables[0].find_all("tr")
    
    # Get all the headings of Lists
    headings = []
    for th in c_tran_table_data[0].find_all("th"):
        # remove any newlines and extra spaces from left and right
        headings.append(th.get_text().replace('\n', ' ').strip())
        
        
    data = {}
    for i in range(len(trip_ids)):
        trip_id = trip_ids[i]
        c_tran_table = c_tran_tables[i]
        c_tran_table_data = c_tran_table.find_all("tr")
        
        table_data = []
        for i in range(1, len(c_tran_table_data)):
            
            t_row = {}
            t_row["Trip id"] = trip_id 
            # find all td's(3) in tr and zip it with t_header
            for td, th in zip(c_tran_table_data[i].find_all("td"), headings): 
                t_row[th] = td.text.replace('\n', '').strip()
            table_data.append(t_row)
        data[trip_id] = table_data
        
    entries = []
    for i in range(len(trip_ids)):
        trip_data_array = data[trip_ids[i]]
        for i in range(len(trip_data_array)):
            entries.append(trip_data_array[i])
    
    #data = json.load(json_string)
    for i in range(len(entries)):
        #record_key = "alice"
        #record_value = json.dumps({'count': n})
        #print("Producing record: {}\t{}".format(record_key, record_value))
        n = json.dumps(entries[i])
        #value_serializer=lambda n: json.
        print("Producing record: ", n)
        producer.produce(topic, n, on_delivery=acked)
        #count += 1
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)
    #print("Produced total of {} messages".format(count))
    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
