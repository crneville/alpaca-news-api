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
    df = retriever.get_news('AAPL', '2017-01-01', '2017-01-20', limit=100)
    pd.set_option('display.max_columns', None)
    print(df)
