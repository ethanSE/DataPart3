#!/usr/bin/env python3
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
from datetime import datetime, timezone, timedelta
import time
import json
import psycopg2
import argparse
import re
import csv
import pandas as pd
import psycopg2.extras as extras
import pytz
from dateutil import parser

DBname = "postgres"
DBuser = "postgres"
DBpwd = "ethan"

def createDataFrame(data):
	#load into dataframe
	stops = pd.DataFrame(data)

	#change stops data types
	stops = stops.astype({'route_id': 'int32', 'trip_id': 'int32', 'vehicle_id': 'int32', 'direction': 'int32', "service_key": 'string'})

	stops["service_key"] = stops["service_key"].apply(serviceKeyModifier)

	return stops

def serviceKeyModifier(key):
	if key == "W":
		return "'Weekday'"
	elif key == "S":
		return "'Saturday'"
	elif key == "U":
		return "'Sunday'"
	else:
		return 'invalid'

def dbconnect():
	# connect to the database
	connection = psycopg2.connect(
		host="localhost",
		database=DBname,
		user=DBuser,
		password=DBpwd,
	)
	connection.autocommit = True
	return connection

def updateOrInsert(data, cursor):
	query = f"UPDATE trip SET route_id = {data.route_id}, direction = {data.direction}, service_key = {data.service_key} WHERE trip_id = {data.trip_id};"
	cursor.execute(query)
	print(cursor.statusmessage)

	if not cursor.statusmessage == "UPDATE 1":
		query = f""" INSERT INTO trip (trip_id,route_id, direction, service_key, vehicle_id) 
			VALUES({data.trip_id},{data.route_id},{data.direction}, {data.service_key}, {data.vehicle_id})
			"""
		cursor.execute(query)
		print(cursor.statusmessage)

def process(messages):
	print("length of messages: ", len(messages))
	
	#put in dataframe
	stops = createDataFrame(messages)
	print("first couple stops")

	#perform validations/assertions
	#TODO
	
	#connect to database
	conn = dbconnect()
	cursor = conn.cursor()
	#iterate over rows and perform updates/insertions
	print("itertuples")
	for row in stops.itertuples():
		updateOrInsert(row, cursor)

if __name__ == '__main__':
	# Read arguments and configurations and initialize
	#args = ccloud_lib.parse_args()
	#config_file = args.config_file
	config_file =  "/home/esam2/.confluent/librdkafka.config"
	topic = "ctranStops"
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
	try:
		messages = []
		while True:
			msg = consumer.poll(1.0)
			if msg is None:
				print("Waiting for message or event/error in poll()")
				continue
			elif msg.error():
				print('error: {}'.format(msg.error()))
			else:
				key = msg.key().decode("utf-8")
				message = msg.value()
				# Check for Kafka message
				if key == "EOT":
					print("received EOT")
					#process current list
					if len(messages) > 0:
						process(messages)
						#for writing to local file
						#store(messages)
					#empty current list
					messages = []
				else:
					#add to current list
					messages.append(json.loads(message))		
	except KeyboardInterrupt:
		pass
	finally:
		# Leave group and commit final offsets
		consumer.close()

# def store(messages):
# 	print("runnning store")
# 	stops = pd.DataFrame(messages)
# 	#change stops data types
# 	stops = stops.astype({'route_id': 'int32', 'trip_id': 'int32', 'vehicle_id': 'int32', 'direction': 'int32'})
# 	stopsCSV = stops.to_csv()
# 	f = open("./stops.csv", "w")
# 	f.write(stopsCSV)
# 	f.close()