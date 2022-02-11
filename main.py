import pandas as pd
import time
import os
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

    tickers = read_tickers('data/tickers.txt')
    retriever = ANR(API_ID, API_KEY)
    for ticker in tickers:
        print(f'--- pulling ticker {ticker} ---')
        df = retriever.get_news(ticker, '2015-01-01', '2022-02-10')
        print(f'--- saving ticker {ticker} ---')
        df.to_csv(f"data/{ticker}_past_data.csv")
        print('--- done ---')
        time.sleep(30)

    print('All Done!')
