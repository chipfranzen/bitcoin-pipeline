#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import yaml
import pickle

import boto3
import pandas as pd

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
               "label": 'Rolling Mean',
               "options": [{"label": "Last Day", "value": "last_day"},
                           {"label": "Last Week", "value": "last_week"}],
               "variable_name": 'rolling_mean',
               "action_id": "plot"}]

    outputs = [{"output_type": "plot",
                "output_id": "plot",
                "on_page_load": True}]

    # get data from s3 and return a dataframe
    def getData(self, params):
        rolling_mean = params['rolling_mean']
        s3 = boto3.resource('s3',
                            aws_access_key_id=access_key_id,
                            aws_secret_access_key=secret_key)
        r = s3.Object('bitcoinprojectbucket', '%s.pkl' % rolling_mean).get()

        df = pickle.loads(r['Body'].read())
        return df

    # plot the dataframe
    def getPlot(self, params):
        df = self.getData(params)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        plt_obj = df.plot()
        plt_obj.set_xlabel('timestamp')
        plt_obj.set_ylabel('USD')
        fig = plt_obj.get_figure()
        return fig


app = BitCoinApp()

if __name__ == '__main__':
    port = int(sys.argv[1])
    app.launch(host='0.0.0.0', port=port)
