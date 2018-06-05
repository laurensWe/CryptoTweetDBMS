import os
import json
import sys
from flink.plan.Environment import get_environment
from flink.plan.Constants import STRING, WriteMode
from flink.functions.GroupReduceFunction import GroupReduceFunction
import time
import pandas as pd
import requests
import csv
import json
from time import gmtime, strftime
url = "https://api.coinmarketcap.com/v1/ticker/"

class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word))


def json_to_tuple(js, fields):
    return tuple([str(js.get(f, '')) for f in fields])

if __name__ == "__main__":
    # get the base path out of the runtime params
    base_path = '/usr/local/Cellar/apache-flink/1.5.0/libexec/examples/python/streaming/data_enrichment'
    keyword_list=['bitcoin, Bitcoin, BITCOIN, BTC, Ethereum, ethereum, ETHEREUM, ETH, Ripple, ripple, RIPPLE, XRP, BitcoinCash, bitcoincash, BITCOINCASH, BCH, Eos, eos, EOS, Litecoin, litecoin, LITECOIN, LTC, Cardano, cardano, CARDANO, ADA, Stellar, STELLAR, stellar, XLM, IOTA, iota, Iota, MIOTA, Tron, tron, TRON, TRX']

    # setup paths to input and output files on disk
    output_file = 'file://' + base_path + '/out.txt'
    output_file1 = 'file://' + base_path + '/out1.txt'
    output_file2 = 'file://' + base_path + '/out2.txt'
    prices_file =  base_path + '/prices.csv'
    twitter_file = 'file://' + base_path + '/twitter1.json'
    # set up the environment with a text file source
    count = 0
    flag = 0 
    flag2 = 0 
    csv_columns = ['id','name','price']
    while count < 3:
        #Get the current prices
        prices = []
        response=requests.get(url)
        for i in range(10):
            name = response.json()[i]["id"]
            price = float(response.json()[i]["price_usd"])
            myCsvRow = {'id':i, 'name': name.strip('"'), 'price': price}
            prices.append(myCsvRow)
        time.sleep(3)
        count = count + 1
    with open(prices_file, 'w') as csv_file:

        for p in prices:
            json.dump(p, csv_file)
            csv_file.write('\n')
    # remove the output file, if there is one there already
    if os.path.isfile(output_file):
        os.remove(output_file)
    
    # set up the environment with a text file source
    env = get_environment()

    twitter_data = env.read_text(twitter_file)
    prices_data = env.read_text(prices_file)

    twitter_data \
        .flat_map(lambda x, c: [(1, word) for word in x.lower().split()]) \
        .filter(lambda x: '#' in x[1]) \
        .group_by(1) \
        .reduce_group(Adder(), combinable=True) \
        .map(lambda y: (str(y[0]), y[1])) \
        .write_csv(output_file1, line_delimiter='\n', field_delimiter=',', write_mode=WriteMode.OVERWRITE)

    twitt_count = env.read_csv(output_file1, types=[STRING, STRING])

    twitt_count \
        .map(lambda x: (x[0], x[1].replace('#', ''))) \
        .write_csv(output_file2, write_mode=WriteMode.OVERWRITE)

    tw = env.read_csv(output_file2, types=[STRING, STRING])

    prices_data \
        .map(lambda x: json_to_tuple(json.loads(x), ['id', 'name', 'price'])) \
        .join(tw).where(1).equal_to(1) \
        .map(lambda x: 'Cryptocurrency: %s price: %s hashtags: %s' % (x[0][2], x[1][1], x[1][0])) \
        .write_text(output_file, write_mode=WriteMode.OVERWRITE)

    env.execute(local=True)
