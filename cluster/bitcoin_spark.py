
#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''spark-submit bitcoin_spark.py'''

import os
import yaml
import pickle

import boto3

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as func


# load credentials
aws_yml = open(os.path.expanduser('~/aws.yml'))
aws_credentials = yaml.load(aws_yml)['AWS']
access_key_id = aws_credentials['aws_access_key_id']
secret_key = aws_credentials['aws_secret_access_key']

if __name__ == '__main__':
    # setup spark session
    sc = SparkContext()
    spark = SparkSession(sc)

    # read normalized data
    df = spark.read.parquet("s3a://bitcoinprojectbucket/parquet_output/*")
    df.cache()

    # define windows for window functions
    hist_window = Window.orderBy(df.timestamp).partitionBy(df.day, df.hour).rowsBetween(-30, 0)
    y_window = Window.orderBy(df.timestamp).partitionBy(df.day, df.hour).rowsBetween(1, 1)

    # get 30 min rolling mean and std
    ml = df.select('timestamp',
                     'day',
                     'hour',
                     'rate',
                     func.mean('rate').over(hist_window).alias('30_min_mean'),
                     func.stddev('rate').over(hist_window).alias('30_min_std'))

    # get target
    ml = ml.select('timestamp',
                     'day',
                     'hour',
                     'rate',
                     '30_min_mean',
                     '30_min_std',
                     func.sum('rate').over(y_window).alias('target'))

    ml.cache()

    # daily data
    dy_ave = df.select(df.date, df.rate).groupBy('date').avg()
    dy_min = df.select(df.date, df.rate).groupBy('date').min()
    dy_max = df.select(df.date, df.rate).groupBy('date').max()
    # join means, mins, maxs
    dy_all = dy_ave.join(dy_max, 'date').join(dy_min, 'date')
    dy_all = dy_all.select(dy_all['date'].alias('timestamp'), dy_all['avg(rate)'].alias('mean'), dy_all['max(rate)'].alias('max'), dy_all['min(rate)'].alias('min'))
    dy_all = dy_all.toPandas()

    # hourly data
    hr_ave = df.select(df.date, df.hour, df.rate).groupBy('date', 'hour').avg().orderBy('date', 'hour', ascending=False).limit(24)
    hr_min = df.select(df.date, df.hour, df.rate).groupBy('date', 'hour').min().orderBy('date', 'hour', ascending=False).limit(24)
    hr_max = df.select(df.date, df.hour, df.rate).groupBy('date', 'hour').max().orderBy('date', 'hour', ascending=False).limit(24)
    # join means, mins, maxs
    hr_all = hr_ave.join(hr_max, ['date', 'hour']).join(hr_min, ['date', 'hour'])
    hr_all =  hr_all.select('date', hr_all['hour'].alias('timestamp'), hr_all['avg(rate)'].alias('mean'), hr_all['max(rate)'].alias('max'), hr_all['min(rate)'].alias('min'))
    hr_all = hr_all.toPandas()

    # get full time series and rolling mean for last day and last week
    rolling_mean =  ml.select('timestamp', '30_min_mean').orderBy('timestamp', ascending=False).limit(10080)
    last_week = df.select('timestamp', 'rate').orderBy('timestamp', ascending=False).limit(10080)
    last_week = last_week.join(rolling_mean, 'timestamp')
    last_week = last_week.toPandas()
    last_day = df.select('timestamp', 'rate').orderBy('timestamp', ascending=False).limit(1440)
    last_day = last_day.join(rolling_mean, 'timestamp')
    last_day = last_day.toPandas()

    # connect to s3 and make puts
    s3 = boto3.resource('s3',
                    aws_access_key_id=access_key_id,
                    aws_secret_access_key=secret_key)

    # dump to pickle strings
    dy_all_put = pickle.dumps(dy_all)
    hr_all_put = pickle.dumps(hr_all)

    last_week_put = pickle.dumps(last_week)
    last_day_put = pickle.dumps(last_day)

    # put to s3
    s3.Object('bitcoinprojectbucket', 'daily.pkl').put(Body=dy_all_put)
    s3.Object('bitcoinprojectbucket', 'hourly.pkl').put(Body=hr_all_put)

    s3.Object('bitcoinprojectbucket', 'last_week.pkl').put(Body=last_week_put)
    s3.Object('bitcoinprojectbucket', 'last_day.pkl').put(Body=last_day_put)


### extra stuff that I didn't get to do:
### Pyspark has no support for user defined aggregate functions, so I couldn't
### do hyperparameter selection for my predictive model over windows of
### timestamps. Once this functionality is supported, I think the function
### below would work:


# import numpy as np
# from sklearn.gaussian_process import GaussianProcessRegressor

# def fit_gpr(data, kernel):
#     n = len(data)
#     data = np.array(data)
#     data.reshape(-1, 1)
#     gpr = GaussianProcessRegressor(n_restarts_optimizer=0, kernel=kernel)
#     X = np.vstack(np.arange(n))
#     gpr.fit(X, data)
#     pred = gpr.predict(np.array(n))
#     return pred

# fit_gpr_spark = func.udf(fit_gpr)

### this could then be used to fit the model with different hyperparameters,
### calculate RMSE over the entire dataset, and pick the best model. I tested
### this idea in pandas on a smaller subset of the data, and it worked pretty
### well.
