# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
import random
import boto3
import time
import math
import csv

STREAM_NAME = "InputStream"
AMPLIFIER = 3
SPIKE_THRESHOLD = 30
PERIOD = math.pi / 5

def make_anomaly(record):
    sign = -1 if random.random() - 0.5 < 0 else 1

    return sign * round(random.random() * AMPLIFIER)

def get_synthetic_data(counter):
    return math.sin(counter * PERIOD)

def generate_synthetic_data(stream_name, kinesis_client):
    counter = 1

    while True:
        data = get_synthetic_data(counter)

        if counter % SPIKE_THRESHOLD == 0:
            data = make_anomaly(data)
        
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=str(data),
            PartitionKey='partitionkey')
        
        time.sleep(0.1)
        counter = counter + 1


def generate_from_dataset(stream_name, kinesis_client, filename):
    with open(filename) as fp:
        reader = csv.reader(fp, delimiter=",", quotechar='"')
        data = [row for row in reader]

    for datapoint in data:
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=str(datapoint[0]),
            PartitionKey='partitionkey')
        
        time.sleep(0.001)

if __name__ == '__main__':
    generate_synthetic_data(STREAM_NAME, boto3.client('kinesis', region_name='us-east-1'))
    # generate_from_dataset(STREAM_NAME, boto3.client('kinesis', region_name='us-east-1'), './datasets/dataset.mat.csv')
