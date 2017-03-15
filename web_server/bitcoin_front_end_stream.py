#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''serve data from stream processor, via s3'''

import sys
import os
import yaml
import pickle
import datetime

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

    inputs = [{"type":"text",
               "key":"words",
               "label":"Short-Term Projections",
               "value":'Over 1, 5, and 10 min',
               "action_id":"simple_html_output"},
              {"input_type": 'dropdown',
               "label": 'Latest Data',
               "options": [{"label": "latest data", "value": "cache"}],
               "variable_name": 'cache',
               "action_id": "plot"}]

    outputs = [{"type":"html",
                "id":"simple_html_output"},
               {"output_type": "plot",
                "output_id": "plot",
                "on_page_load": True}]

    # get projections and create html table
    def getHTML(self, params):
        words = params["words"]
        s3 = boto3.resource('s3',
                            aws_access_key_id=access_key_id,
                            aws_secret_access_key=secret_key)
        r = s3.Object('bitcoinprojectbucket', 'projections.pkl').get()
        data = pickle.loads(r['Body'].read())
        timestamp = data[-1]
        projections = data[:-1]
        table = [['current time', str(datetime.datetime.now())],
                ['timestamp', timestamp],
                ['1-min', projections[0]],
                ['5-min', projections[1]],
                ['10-min', projections[2]]]
        table = pd.DataFrame(table)
        table = table.set_index(0)
        table.columns = ['projections']

        return table.to_html()

    # get latest records
    def getData(self, params):
        cache = params['cache']
        s3 = boto3.resource('s3',
                            aws_access_key_id=access_key_id,
                            aws_secret_access_key=secret_key)
        r = s3.Object('bitcoinprojectbucket', 'cache.pkl').get()
        data = pickle.loads(r['Body'].read())
        df = pd.DataFrame(data)
        return df

    # plot latest records
    def getPlot(self, params):
        df = self.getData(params)
        n = len(df)
        df = df.set_index(-np.arange(n))
        df = df.sort_index()
        plt_obj = df.plot()
        plt_obj.set_xlabel('minutes before now')
        plt_obj.set_ylabel('USD')
        fig = plt_obj.get_figure()
        return fig

app = BitCoinApp()

if __name__ == '__main__':
    port = int(sys.argv[1])
    app.launch(host='0.0.0.0', port=port)
