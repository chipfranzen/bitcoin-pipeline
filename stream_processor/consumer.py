#! /usr/bin/env python
# -*- coding: <utf-8> -*-

'''kinesis consumer for bitcoin stream'''

import sys
import os
import time
import yaml
import json
import datetime
import pickle

import numpy as np
import boto3

from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RationalQuadratic

# load credentials
aws_yml = open(os.path.expanduser('~/aws.yml'))
aws_credentials = yaml.load(aws_yml)['AWS']
access_key_id = aws_credentials['aws_access_key_id']
secret_key = aws_credentials['aws_secret_access_key']


# converts timestamp string to datetime
def convert_timestamp(x):
    return datetime.datetime.strptime(x, '%b %d, %Y %H:%M:%S %Z')


# initalize predictive model
gpr = GaussianProcessRegressor(kernel=RationalQuadratic(),
                               n_restarts_optimizer=3)

if __name__ == '__main__':
    # intialize cache
    cache = set()
    # setup kinesis consumer client
    stream_name = 'bitcoin_stream'
    client = boto3.client('kinesis', 'us-west-2',
                          aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_key)
    shard_id = client.describe_stream(StreamName=stream_name)['StreamDescription']['Shards'][0]['ShardId']

    # startup (first minute, populating the cache)
    shard = client.get_shard_iterator(StreamName=stream_name,
                                      ShardId=shard_id,
                                      ShardIteratorType='AT_TIMESTAMP',
                                      Timestamp=time.time() - 3600)
    for _ in xrange(60):
        # get shard iterator
        shard_it = shard['ShardIterator']
        # kinesis only allows 5 requests per second
        for _ in range(4):
            # get records
            records = client.get_records(ShardIterator=shard_it, Limit=10000)
            # parse records
            data = records['Records']
            for r in data:
                r = json.loads(r['Data'])
                updated = convert_timestamp(r['time']['updated'])
                rate = r['bpi']['USD']['rate_float']
                # add extracted data to cache
                cache.add((updated, rate))
            # get next shard iterator
            shard_it = records['NextShardIterator']
        # wait 1 second to keep requests per second at 5
        time.sleep(1)

    # after initialization
    # set timer
    if sys.argv[1]:
        timer = int(sys.argv[1])
    else:
        timer = np.inf
    # set up a counter for successful puts
    putcount = 0
    while timer > 0:
        try:
            # limit 5 requests per second
            for _ in range(4):
                # get records
                records = client.get_records(ShardIterator=shard_it,
                                             Limit=10000)
                # parse records
                data = records['Records']
                for r in data:
                    r = json.loads(r['Data'])
                    updated = convert_timestamp(r['time']['updated'])
                    rate = r['bpi']['USD']['rate_float']
                    # add data to cache
                    cache.add((updated, rate))
                # get next shard_id
                shard_it = records['NextShardIterator']
            # sort cache by timestamp
            cache_list = sorted(list(cache), key=lambda x: x[0], reverse=True)
            # truncate at last hour of data
            if len(cache) > 60:
                cache_list = cache_list[:60]

            ## make predictions with model
            # set up data
            n = len(cache_list)
            X = np.vstack(np.arange(n))
            X.reshape(-1, 1)
            y = np.array([e[1] for e in cache_list])

            # fit model and make predictions
            gpr.fit(X, y)
            # 1, 5, and 10 minute predictions
            pred = gpr.predict(np.vstack(np.array([n, n+5, n+10])))

            # package into a string for storage on s3
            pred = list(pred)
            pred.append(cache_list[0][0])
            proj_put = pickle.dumps(pred)

            cache_put = pickle.dumps(cache_list)

            # connect to s3
            s3 = boto3.resource('s3',
                                aws_access_key_id=access_key_id,
                                aws_secret_access_key=secret_key)
            # put records
            s3.Object('bitcoinprojectbucket', 'projections.pkl').put(Body=proj_put)
            s3.Object('bitcoinprojectbucket', 'cache.pkl').put(Body=cache_put)
            # increment counts
            timer -= 1
            putcount += 1
            # wait 1 minute
            time.sleep(60)
        except:
            print 'failed put'
            # increment counts
            timer -= 1
            # wait 1 minute
            time.sleep(60)
    # see put success count
    print('Made', putcount, 'puts.')
