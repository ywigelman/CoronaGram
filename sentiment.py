import requests
import json
from conf import *


class PostText(object):
    def __init__(self, text: tuple, content_type: str = CONTENT_TYPE, accept_encoding: str = ACCEPT_ENCODING,
                 x_rapidapi_key: str = X_RAPID_API_KEY, x_rapidapi_host: str = X_RAPID_API_HOST,
                 url_language: str = URL_LANGUAGE, url_translation: str = URL_TRANSLATION,
                 sentiment_key: str = SENTIMENT_SUBSCRIPTION_KEY, url_sentiment: str = URL_SENTIMENT):
        """
        PostText is an object that for parsing, translating and analysing post sentiment
        :param text: str object that represents instagram post record
        :param content_type: str object that represents content type for translate api
        :param accept_encoding: str object that represents encoding for translate api
        :param x_rapidapi_key: str object that represents key for rapid-api
        :param x_rapidapi_host: str object that represents the rapid-api host address
        :param url_language: str object that represents the url for language detection
        :param url_translation: str object that represents the url for translation
        :param sentiment_key: str object that represents the key for sentiment analysis
        :param url_sentiment: str object that represents the url for sentiment analysis
        """
        self.text = text
        self.translate_headers = {'content-type': content_type, 'accept-encoding': accept_encoding,
                                  'x-rapidapi-key': x_rapidapi_key, 'x-rapidapi-host': x_rapidapi_host}
        self.sentiment_headers = {'Ocp-Apim-Subscription-Key': sentiment_key}
        self.url_language = url_language
        self.url_translation = url_translation
        self.url_sentiment = url_sentiment
        self.clean = False
        self.language = None
        self.payload = None
        self.translation = None
        self.sentiment = None

    def _text_clean(self):
        """
        a function for parsing post text item to contain only the content
        :return: None
        """
        self.text = eval(self.text[0])[0]['node']['text']
        self.clean = True

    def detect_language(self):
        """
        a function that connects with an api to detect the language of a post
        :return: None
        """
        if not self.clean:
            self._text_clean()
        self.payload = "q={}".format(self.text)
        resp = requests.request('POST', self.url_language, data=self.payload.encode('utf-8'),
                                headers=self.translate_headers)
        self.language = json.loads(resp.text)['data']['detections'][0][0]['language']

    def translate(self, to_lang: str = TARGET_LANG):
        """
        a function for translating a post to a given language
        :param to_lang: str object that represents a target language for translation
        :return: None
        """
        if not self.language:
            self.detect_language()
        self.payload += '&source={}&target={}'.format(self.language, to_lang)
        resp = requests.request('POST', self.url_translation, data=self.payload.encode('utf-8'),
                                headers=self.translate_headers)
        self.translation = json.loads(resp.text)['data']['translations'][0]['translatedText']

    def analyze_sentiment(self, lang: str = TARGET_LANG):
        """
        a function for analyzing a sentiments from a text (either positive, negative or neutral)
        :param lang: the language to analyze from
        :return: None
        """
        if not self.translation:
            self.translate()
        query = {"documents": [
            {"id": "1", "language": "{}".format(lang),
             "text": "{}".format(self.translation)}
        ]}
        response = requests.post(self.url_sentiment, headers=self.sentiment_headers, json=query)
        self.sentiment = response.json()['documents'][0]['sentiment']
