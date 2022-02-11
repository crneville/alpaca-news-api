import pandas as pd
import os
from AlpacaNewsRetriever.NewsRetriever import AlpacaNewsRetriever as ANR
from dotenv import load_dotenv
load_dotenv()
API_ID = os.environ['API_ID']
API_KEY = os.environ['API_KEY']


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    retriever = ANR(API_ID, API_KEY)
    df = retriever.get_news('AAPL', '2015-01-01', '2016-01-22')
    pd.set_option('display.max_columns', None)
    print(df)
