#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''front end to serve mins maxs and means'''

import sys
import os
import yaml
import pickle

import boto3
import pandas as pd
import numpy as np

from spyre import server

# load credentials
aws_yml = open(os.path.expanduser('~/aws.yml'))
aws_credentials = yaml.load(aws_yml)['AWS']
access_key_id = aws_credentials['aws_access_key_id']
secret_key = aws_credentials['aws_secret_access_key']


class BitCoinApp(server.App):
    # set app parameters
    title = 'Bitcoin Exchange Rates'

    inputs = [{"input_type": 'dropdown',
               "label": 'High/Low',
               "options": [{"label": "Daily", "value": "daily"},
                           {"label": "Hourly", "value": "hourly"}],
               "variable_name": 'min_max',
               "action_id": "plot"}]

    outputs = [{"output_type": "plot",
                "output_id": "plot",
                "on_page_load": True}]

    # get data from s3 and return a dataframe
    def getData(self, params):
        min_max = params['min_max']
        s3 = boto3.resource('s3',
                            aws_access_key_id=access_key_id,
                            aws_secret_access_key=secret_key)
        r = s3.Object('bitcoinprojectbucket', '%s.pkl' % min_max).get()

        df = pickle.loads(r['Body'].read())
        return df

    # plot the dataframe
    def getPlot(self, params):
        df = self.getData(params)
        try:
            df = df.drop('timestamp', axis=1)
        except:
            df = df
        n = len(df)
        df = df.set_index(-np.arange(n))
        df = df.sort_index()
        plt_obj = df.plot()
        plt_obj.set_xlabel('periods from now')
        plt_obj.set_ylabel('USD')
        fig = plt_obj.get_figure()
        return fig


app = BitCoinApp()

if __name__ == '__main__':
    port = int(sys.argv[1])
    app.launch(host='0.0.0.0', port=port)
