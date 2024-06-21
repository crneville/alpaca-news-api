# %%
import pandas as pd
import datetime
import time
import random
import os, sys
import json
from tqdm import tqdm
from AlpacaNewsRetriever.NewsRetriever import AlpacaNewsRetriever as ANR
from dotenv import load_dotenv

script_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))

def read_tickers(DIR):
    with open(DIR) as file:
        lines = file.readlines()
        ticker_list = [line.rstrip() for line in lines]
        return ticker_list

def load_creds_from_file(filepath):
    with open(filepath, 'r') as creds_file:
        return [c.replace('\n','').replace('\r','').strip() for c in creds_file.readlines()]

def load_default_creds():
    load_dotenv(os.path.join(script_dir, '.env'))
    return os.environ['API_ID'], os.environ['API_KEY']

def filter_tickers(tickers, filters):
    filters = [f.lower() for f in filters.split(',')]
    return [ticker for ticker in tickers if ticker.lower()[0] in filters]

# %%
# NOTE: https://github.com/microsoft/vscode-jupyter/issues/1837
is_vscode = 'VSCODE_CWD' in os.environ.keys()
if is_vscode: sys.argv = ['']
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse, pyarrow
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-content', action='store_true')
    parser.add_argument('--reverse', action='store_true')
    parser.add_argument('--shuffle', action='store_true')
    parser.add_argument('--start', default='2015-01-01')
    parser.add_argument('--end', default=f'{pd.Timestamp.now().now().normalize():%Y-%m-%d}')
    parser.add_argument('--no-update', action='store_true')
    parser.add_argument('--ticker-db', default=None)
    parser.add_argument('--tickers', default=None)
    parser.add_argument('--crypto', action='store_true')
    parser.add_argument('--creds', default=None)
    config = parser.parse_args()

    if not config.crypto and config.ticker_db and 'crypto' in config.ticker_db.lower():
        config.crypto = True

    if 'API_ID' in os.environ and 'API_KEY' in os.environ:
        api_id, api_key = os.environ['API_ID'], os.environ['API_KEY']
    elif config.creds is not None:
        api_id, api_key = load_creds_from_file(config.creds)
    else:
        api_id, api_key = load_default_creds()

    script_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
    retriever = ANR(api_id, api_key, include_content=not config.no_content)
    
    tickerfilename = config.ticker_db if config.ticker_db else ('tickers.txt' if not config.crypto else 'cryptotickers.txt')
    tickers = read_tickers(os.path.join(script_dir, tickerfilename))

    if config.tickers is not None:
        tickers = filter_tickers(tickers, config.tickers)

    if config.reverse:
        tickers = tickers[::-1]
    if config.shuffle:
        random.shuffle(tickers)

    datadir = os.path.join(script_dir, 'data')
    if config.crypto: datadir = os.path.join(datadir, 'crypto')

    desc = 'Fetching Historical News'
    pbar = tqdm(tickers, desc=desc)

    for ticker in pbar:
        pbar.set_description(desc+f' for {ticker}', refresh=True)
        try:
            filepath = os.path.join(datadir, f"{ticker}.parquet")
            file_exists = os.path.isfile(filepath)
            start = config.start
            
            oldnews = pd.DataFrame()
            if file_exists:
                try:
                    oldnews = pd.read_parquet(filepath)
                except pyarrow.lib.ArrowInvalid:
                    pass
                if not oldnews.empty and not config.no_update:
                    start = f'{oldnews.index.max().normalize():%Y-%m-%d}'

            if config.crypto:
                news = retriever.get_news(ticker.replace('-',''), start, config.end)
            else:
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
