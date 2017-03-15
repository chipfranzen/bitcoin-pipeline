#! /usr/bin/env python
# -*- coding: <utf-8> -*-

'''proof of concept for bitcoin rate data stream. not used in production'''

import datetime
import time

import requests

if __name__ == '__main__':
    while True:
        # make request
        r = requests.get('https://api.coindesk.com/v1/bpi/currentprice.json')
        # check status code
        if r.status_code == 200:
            # get json
            data = r.json()
            # get price
            price = data['bpi']['USD']['rate_float']
            # get timestamp
            timestamp = data['time']['updated']
            # format timestamp
            timestamp = datetime.datetime.strptime(timestamp,
                                                   '%b %d, %Y %H:%M:%S %Z')
            # write data
            with open('bitcoin_price_index.txt', 'a') as f:
                f.write(str(timestamp) + '\t' + str(price) + '\n')
        # sample every minute
        time.sleep(60)
