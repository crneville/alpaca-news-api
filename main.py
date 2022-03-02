import pandas as pd
import datetime
import time
import os
from tqdm import tqdm
from AlpacaNewsRetriever.NewsRetriever import AlpacaNewsRetriever as ANR
from dotenv import load_dotenv
load_dotenv()

API_ID = os.environ['API_ID']
API_KEY = os.environ['API_KEY']

def read_tickers(DIR):
    with open(DIR) as file:
        lines = file.readlines()
        ticker_list = [line.rstrip() for line in lines]
        return ticker_list

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--content', action='store_true')
    parser.add_argument('--start', default='2015-01-01')
    parser.add_argument('--end', default=f'{datetime.datetime.now()-datetime.timedelta(days=1):%Y-%m-%d}')
    config = parser.parse_args()

    script_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
    tickers = read_tickers(os.path.join(script_dir, 'data', 'tickers.txt'))
    retriever = ANR(API_ID, API_KEY, include_content=config.content)

    desc = 'Fetching Historical News'
    pbar = tqdm(tickers, desc=desc)
    for ticker in pbar:
        pbar.set_description(desc+f' for {ticker}', refresh=True)
        df = retriever.get_news(ticker, config.start, config.end)
        if df is not None:
            df.to_csv(f"data/{ticker}.csv")
    print('All Done!')
