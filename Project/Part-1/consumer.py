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
from json import loads


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
    #consumer_conf['group_id'] = 'python_example_group_2'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    JsonList = []
    count = 0
    key = 5
    #print("key is ",key)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Total records consumed is ",count)
                print("Waiting for message or event/error in poll()")
                if len(JsonList) > 0:
                    json = json.dumps(JsonList)
                    with open("/home/hegde/examples/clients/cloud/python/results/storage_result.json", "w") as outfile:
                        outfile.write(json)
                    JsonList.clear()
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                #print(record_key)
                #if '5' in str(record_key):
                record_value = msg.value()
                    #count += 1
                #print("record key is ", record_key)
                #print("record value is ", record_value)
                data = json.loads(record_value)
                JsonList.append(data)
                count += 1
                #count = data['count']
                #total_count += count
                    #print("Consumed record with key {} and value {}, \
                      #and record is {}"
                      #.format(record_key, record_value, data))
                #msg = json.loads(msg.decode('utf-8'))
                #msg = json.loads(msg.decode('utf-8'))
                #print(type(msg))
                #print("Consumed record is: ",data)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
