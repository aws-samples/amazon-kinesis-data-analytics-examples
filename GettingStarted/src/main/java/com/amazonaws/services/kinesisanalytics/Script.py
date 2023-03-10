import json
import random
import boto3

STREAM_NAME = "window-data-analysis-cli"


def get_data(i,j):
    return {
        'groupId': "group_"+str(j),
        'rowId':"row_Id"+str(i),
        'amount': round(random.random() * 100, 2)}


def generate(stream_name, kinesis_client):
    i=1
    j=1
    while True:
        data = get_data(i,j)
        print(data)
        i+=1
        if(i%10==0):
            j+1
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey="partitionkey")


if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis', region_name='us-east-1'))