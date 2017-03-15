#! /usr/bin/env python
# -*- coding: <utf-8> -*-

'''makes api requests and puts to kinesis'''

import datetime
import time
import json
import os

import yaml
import requests
import boto3

aws_yml = open(os.path.expanduser('~/aws.yml'))
aws_credentials = yaml.load(aws_yml)['AWS']

if __name__ == '__main__':
    # make request
    r = requests.get('https://api.coindesk.com/v1/bpi/currentprice.json')
    # check status code
    if r.status_code == 200:
        # get json
        data = r.json()
        print(data)
        data = (json.dumps(data, encoding='utf-8')+'\n').encode('utf-8')
        client = boto3.client('kinesis', 'us-west-2',
                              aws_access_key_id=aws_credentials['aws_access_key_id'],
                              aws_secret_access_key=aws_credentials['aws_secret_access_key'])
        response = client.put_record(StreamName='bitcoin_stream',
                                     Data= data,
                                     PartitionKey='A')
        client = boto3.client('firehose', 'us-west-2',
                              aws_access_key_id=aws_credentials['aws_access_key_id'],
                              aws_secret_access_key=aws_credentials['aws_secret_access_key'])
        response = client.put_record(DeliveryStreamName='bitcoin_kinesis',
                                     Record= {'Data': data})
