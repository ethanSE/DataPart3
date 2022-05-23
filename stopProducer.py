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
from datetime import datetime, timezone, timedelta
import json
import re
import urllib.request
from bs4 import BeautifulSoup

def getTripInfo(soup):
    trip_info = getTableInfo(soup)
    trip_ids = getTripIds(soup)

    for trip_id, info in zip(trip_ids, trip_info):
        info["trip_id"] = trip_id
    return trip_info

def getTableInfo(soup):
    def infoExtractor(table):
        row = table.find_all('tr')[1]
        cells = row.findAll("td")
        vehicle_id = int(cells[0].get_text())
        route_id = int(cells[3].get_text())
        
        try:
            direction = int(cells[4].get_text())
        except:
            #mark direction as -1 - not sure if this data should be thrown out or not
            direction = -1
           
        service_key = cells[5].get_text()
        return { "route_id": route_id, "vehicle_id": vehicle_id, "service_key": service_key, "direction": direction, "trip_id": None }
    return list(map(infoExtractor, soup.find_all('table')))

def getTripIds(soup):
    def trip_id_extractor(t):
        number_extractor = re.compile('([0-9]{9})')
        return int(re.search(number_extractor, t.get_text()).group(0))
        
    return list(map(trip_id_extractor, soup.find_all('h3')))

def getSoup(source):
    if source == "query":
        url = 'http://www.psudataeng.com:8000/getStopEvents'
        html = urllib.request.urlopen(url)
        return BeautifulSoup(html, "html.parser")

    else:
        #read from local file - for development
        htmlFile = open("./html.html", "r")
        return BeautifulSoup(htmlFile.read(), features="lxml")

if __name__ == '__main__':
    config_file = "/home/esam2/.confluent/librdkafka.config"
    topic = "ctranStops"
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

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

    #parse html and get list of tripinfo objects
    trip_info = getTripInfo(getSoup("query"))

    delivered_records = 0

    for n in range(len(trip_info)):
        record_key = "key"
        record_value = json.dumps(trip_info[n])
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(0)

    producer.produce(topic, key="EOT", value="")
    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))

