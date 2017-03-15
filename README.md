# bitcoin-pipeline

## Charles Franzen

Please see the ipython notebook for an overview of the project


file explanations:

* `bitcoin_price_index.txt` is a sample of the raw data

* `http_requester/` contains programs for making api requests and putting to kinesis

* `cluster/` contains a script for normalizing data, and for batch processing

* `stream_processor/` contains a script for a kinesis consumer

* `web_server/` contains 3 web apps to serve the 3 pages of the front end