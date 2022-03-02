import time
import urllib.parse
import requests
import pandas as pd
from tqdm import tqdm

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
    def __init__(self, API_ID, API_KEY, include_content=False):
        self.content = include_content
        self.API_ID = API_ID
        self.API_KEY = API_KEY
        self.base_url = 'https://data.alpaca.markets/v1beta1/news?'

    """
    def get_news(self, symbol, start, end, limit=50):
        raw_response = self.get_raw_request(symbol, start, end, limit)
        if limit <= 50:
            print(raw_response)
            return self.post_process(raw_response, symbol)
        else:
            # TODO: add pagination to API call
            return raw_response
    """
    def get_news(self, symbol, start, end, limit=50, max_call_per_min=999):
        raw_response = self.get_raw_request(symbol, start, end, limit)
        if self._is_over_limit(raw_response):
            self._wait_for_api_calls_to_reset()
            raw_response = self.get_raw_request(symbol, start, end, limit)

        if self._is_invalid_symbol(raw_response): return None

        token = raw_response['next_page_token']
        df = self.post_process(raw_response, symbol)
        num_call = 1 # number of api calls. Must make sure it is less than

        while token is not None:
            if num_call >= max_call_per_min:
                self._wait_for_api_calls_to_reset()
                num_call = 0
            raw_response = self.get_raw_request(symbol, start, end, limit, token=token)
            if self._is_over_limit(raw_response):
                num_call = max_call_per_min
            else:
                token = raw_response['next_page_token']
                df = pd.concat([df, self.post_process(raw_response, symbol)], ignore_index=True)
                num_call += 1
        return df

    def _is_invalid_symbol(self, response):
        return 'message' in response and 'invalid symbol' in response['message']

    def _is_over_limit(self, response):
        return 'message' in response and 'too many requests' in response['message']

    def _wait_for_api_calls_to_reset(self):
        for t in tqdm(range(60), desc='API call limit reached; waiting', leave=False):
            time.sleep(1)

    def get_raw_request(self, symbol, start, end, limit, token=None):
        url = self.base_url
        url += f'start={start}&end={end}&symbols={urllib.parse.quote(symbol)}&limit={limit}&include_content={self.content}'
        if token is not None:
            url += f'&page_token={token}'
        response = requests.get(url, headers={"Apca-Api-Key-Id": self.API_ID, 'Apca-Api-Secret-Key': self.API_KEY})
        return response.json()

    def post_process(self, content, symbol):
        news_dict = {'ticker':[], 'timestamp':[], 'headline':[], 'summary':[]}
        if self.content: news_dict['content'] = []
        for news in content['news']:
            news_dict['ticker'].append(symbol)
            news_dict['timestamp'].append(news['created_at'])
            news_dict['headline'].append(news['headline'])
            news_dict['summary'].append(news['summary'])
            if self.content:
                news_dict['content'].append(news['content'] if 'content' in news else '')
        cols = ['ticker', 'timestamp', 'headline', 'summary']
        if self.content: cols += ['content']
        return pd.DataFrame(news_dict, columns=cols)
