#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
spark-submit bitcoin_data_normalizer.py'''

import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession

def convert_timestamp(x):
    # converts timestamps into datetimes
    date = datetime.datetime.strptime(x[0], '%b %d, %Y %H:%M:%S %Z')
    return date, date.date(), date.month, date.day, date.hour, x[1]

if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)

    # get raw data
    raw_data = spark.read.json("s3a://bitcoinkinesisdump/*/*/*/*/*")

    # select info
    usd = raw_data.select(raw_data.time.updated.alias('timestamp'), raw_data.bpi.USD.rate_float.alias('rate'))
    usd = spark.createDataFrame(usd.rdd.map(lambda x: convert_timestamp(x)), ['timestamp', 'date', 'month', 'day', 'hour', 'rate'])

    # write to s3
    usd.write.parquet("s3a://bitcoinprojectbucket/parquet_output/", mode='overwrite')
