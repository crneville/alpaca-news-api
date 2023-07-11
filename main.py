# %%
import pandas as pd
import datetime
import time
import os, sys
import json
from tqdm import tqdm
from AlpacaNewsRetriever.NewsRetriever import AlpacaNewsRetriever as ANR
from dotenv import load_dotenv

script_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
load_dotenv(os.path.join(script_dir, '.env'))

API_ID = os.environ['API_ID']
API_KEY = os.environ['API_KEY']

def read_tickers(DIR):
    with open(DIR) as file:
        lines = file.readlines()
        ticker_list = [line.rstrip() for line in lines]
        return ticker_list

# %%
# NOTE: https://github.com/microsoft/vscode-jupyter/issues/1837
is_vscode = 'VSCODE_CWD' in os.environ.keys()
if is_vscode: sys.argv = ['']
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-content', action='store_true')
    parser.add_argument('--start', default='2015-01-01')
    parser.add_argument('--end', default=f'{datetime.datetime.now()-datetime.timedelta(days=1):%Y-%m-%d}')
    config = parser.parse_args()

    script_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
    tickers = read_tickers(os.path.join(script_dir, 'tickers.txt'))
    retriever = ANR(API_ID, API_KEY, include_content=not config.no_content)

    desc = 'Fetching Historical News'
    pbar = tqdm(tickers, desc=desc)
    for ticker in pbar:
        pbar.set_description(desc+f' for {ticker}', refresh=True)
        try:
            filepath = os.path.join(script_dir, 'data', f"{ticker}.parquet")
            file_exists = os.path.isfile(filepath)
            start = config.start
            oldnews = pd.DataFrame()
            if file_exists:
                oldnews = pd.read_parquet(filepath)
                if not oldnews.empty:
                    start = f'{oldnews.index.max() + pd.Timedelta(days=1):%Y-%m-%d}'

            news = retriever.get_news(ticker, start, config.end)
            
            if news is not None:
                news['timestamp'] = pd.to_datetime(news['timestamp'])
                news = news.set_index('timestamp').sort_index()
                if not oldnews.empty:
                    news = pd.concat((oldnews, news))
                news.reset_index().drop_duplicates().set_index('timestamp').sort_index().to_parquet(filepath)
        except json.decoder.JSONDecodeError as ex:
            print(f'JSON Error with {ticker}: {ex}')
    print('All Done!')
