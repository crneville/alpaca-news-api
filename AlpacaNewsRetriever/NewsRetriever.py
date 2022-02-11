import requests
import pandas as pd


class AlpacaNewsRetriever:
    """
    Class for getting historical news from Alpaca
    __init__():
        API_ID: your Alpaca ID
        API_KEY: your Alpaca secret key

    get_news():
        symbol: Ticker of the stock. e.g. AAPL
        start: start timestamp in RFC 3339 format. e.g. 01-01-2015
        end: end timestamp in RFC 3339 format. e.g. 01-01-2019
        limit: number of news per page, max 50.

        return: a pandas dataframe contains the news
    """
    def __init__(self, API_ID, API_KEY):
        self.API_ID = API_ID
        self.API_KEY = API_KEY
        self.base_url = 'https://data.alpaca.markets/v1beta1/news?'

    def get_news(self, symbol, start, end, limit=50):
        raw_response = self.get_raw_request(symbol, start, end, limit)
        if limit <= 50:
            print(raw_response)
            return self.post_process(raw_response, symbol)
        else:
            # TODO: add pagination to API call
            return raw_response

    def get_raw_request(self, symbol, start, end, limit):
        url = self.base_url
        url += f'start={start}&end={end}&symbols={symbol}&limit={limit}'
        response = requests.get(url, headers={"Apca-Api-Key-Id": self.API_ID, 'Apca-Api-Secret-Key': self.API_KEY})
        return response.json()

    def post_process(self, content, symbol):
        dict = {'ticker':[], 'timestamp':[], 'headline':[], 'summary':[]}
        for news in content['news']:
            dict['ticker'].append(symbol)
            dict['timestamp'].append(news['created_at'])
            dict['headline'].append(news['headline'])
            dict['summary'].append(news['summary'])
        df = pd.DataFrame([dict['ticker'], dict['timestamp'], dict['headline'], dict['summary']]).transpose()
        df.columns = ['ticker', 'timestamp', 'headline', 'summary']
        return df
