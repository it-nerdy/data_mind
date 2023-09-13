import json
import logging
import boto3
import sys
from elasticsearch import Elasticsearch

queue_url = 'https://sqs.eu-west-1.amazonaws.com/287820185021/openaq-mallikarjun'

# Setup SQS
sqs = boto3.client('sqs', region_name='eu-west-1',
                   aws_access_key_id='AKIAUGA3LPW6V44QSUO5',
                   aws_secret_access_key='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'
                   )


def setup_elasticsearch():
    # Setup Elasticsearch connection
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    index_name = "airquality-data-2"
    with open('mapping.json', 'r') as mapping_file:
        mapping = json.load(mapping_file)
        if mapping:
            if not es.indices.exists(index_name):
                es.indices.create(index=index_name, body=mapping)
                return es
        else:
            logging.info("mapping is null exiting the program")
            sys.exit(1)


def transform_coordinates(data):
    # Check if 'coordinates' key exists and has both 'latitude' and 'longitude'
    if "coordinates" in data and "latitude" in data["coordinates"] and "longitude" in data["coordinates"]:
        lat = data["coordinates"].pop("latitude")
        lon = data["coordinates"].pop("longitude")
        data["coordinates"]["lat"] = float(lat)
        data["coordinates"]["lon"] = float(lon)
    return data


while True:
    # Fetch messages from SQS
    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1  # Adjust as needed
    )
    es = setup_elasticsearch()
    if 'Messages' in messages:
        for message in messages['Messages']:
            # Decode the SQS message
            data = json.loads(message['Body'])
            air_quality_data = transform_coordinates(json.loads(data['Message']))

            # Index data into Elasticsearch
            es.index(index="airquality-data-2", body=air_quality_data)

            # Delete processed message from SQS
            # sqs.delete_message(
            #     QueueUrl=queue_url,
            #     ReceiptHandle=message['ReceiptHandle']
            # )
