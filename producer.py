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
from datetime import datetime, timezone, timedelta, date
import json
import urllib.request

def request():
	url = 'http://www.psudataeng.com:8000/getBreadCrumbData'
	with urllib.request.urlopen(url) as response:
		return response.read()

if __name__ == '__main__':
	config_file = "/home/esam2/.confluent/librdkafka.config"
	topic = "ctran"
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

	html = request()
	data = json.loads(html)

	#implement logging
	logobj = {
		"date": date.today().strftime("%m/%d/%y"),
		"sensorReadings": len(data),
	}
	f = open("./producerLog.txt", "a")
	f.write(json.dumps(logobj))
	f.write("\n")
	f.close()

	for n in range(len(data)):
		record_key = "key"
		record_value = json.dumps(data[n])
		producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
		producer.poll(0)
		if (n%10000 == 0 ):
			producer.produce(topic, key="EOT", value="")
			producer.flush()
			print(n)

	producer.produce(topic, key="EOT", value="")
	producer.flush()

	print("{} messages were produced to topic {}!".format(delivered_records, topic))
