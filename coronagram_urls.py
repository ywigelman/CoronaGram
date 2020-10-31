from bs4 import BeautifulSoup as bs
import time
import json
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import Chrome, DesiredCapabilities
from random import randint, choice
import pickle
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
import multiprocessing
import pandas as pd
from urllib.request import urlopen
from pandas.io.json import json_normalize
import numpy as np

#  todo consider putting the project inside a docker with a version of chrome or other browser and matching key

HASHTAG_CORONA_URL = 'https://www.instagram.com/explore/tags/corona/?h__a=1'  # hashtag corona page
POST_URL_PREFIX = 'https://www.instagram.com/p'
SCROLL_HEIGHT = 'return document.body.scrollHeight'  # java scrip command for getting scroll height
SCROLL_2_BOTTOM = 'window.scrollTo(0, document.body.scrollHeight);'  # java scrip command for scrolling to page bottom
CHROME_HEADLESS = '--headless'  # java scrip command for making a headless browser (no gui)
URL_KEY_WORD = 'window._sharedData = '


class ChromeScraper(object):

    def __init__(self, url: str, headless_mode, implicit_wait):
        self.url = url
        self.headless = headless_mode
        self.implicit_wait = implicit_wait
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument(CHROME_HEADLESS)
        self.driver = Chrome(options=chrome_options)
        self.driver.implicitly_wait(self.implicit_wait)
        self.driver.get(self.url)
        self.scrap_lst = []
        self.pre_scroll_height = self.driver.execute_script(SCROLL_HEIGHT)  # stores the height of the page

    @property
    def body(self):
        """
        a property method for getting the content (body) of an html page
        :return:
        """
        source = self.driver.page_source
        data = bs(source, 'html.parser')
        return data.find('body')

    @property
    def last_item(self):
        return self.scrap_lst[-1]

    @ property
    def items_scraped(self) -> int:
        return len(self.scrap_lst)


class HashTagPage(ChromeScraper):

    def __init__(self, url: str, headless_mode: bool = True, implicit_wait: int = 30,
                 scroll_pause_time_range: tuple = (10, 30), previous_session_last_url_scraped: str = None):

        self.scroll_pause_time_range = scroll_pause_time_range  # random wait before scroll down
        self.headless = headless_mode
        self.implicit_wait = implicit_wait
        self.last_url_scraped = previous_session_last_url_scraped
        self.url_lst = []
        super().__init__(url, headless_mode=self.headless, implicit_wait=self.implicit_wait)

    def all_urls(self):
        """
        generator method is used for scraping data from pages with infinite scrolling
        :return:
        """
        while True:
            self.driver.execute_script(SCROLL_2_BOTTOM)  # Scroll to the bottom of the page
            time.sleep(randint(*self.scroll_pause_time_range))  # wait between each scroll
            # Calculate new scroll height and compare with last scroll height
            post_scroll_height = self.driver.execute_script(SCROLL_HEIGHT)
            if post_scroll_height == self.pre_scroll_height:  # todo check which exception one gets when reaching the bottom page and use try except
                # If heights are the same it will exit the function
                break
            self.pre_scroll_height = post_scroll_height
            for url in self.get_urls():
                if not url:
                    return
                elif url in self.url_lst:  # it seems that in hashtag pages that are posts that are reused
                    continue
                self.url_lst.append(url)
                yield url  # todo  perhaps yield after reaching a certain bach size and after proxies can scrape in paralle
                if self.items_scraped > 1000: return

    def get_urls(self):
        """
        generator method for scraping urls from instagram hashtag pages
        :return:
        """
        #yield filter(lambda element: '/p/' in element.get_attribute('href'), self.driver.find_elements_by_tag_name('a'))
        for element in self.driver.find_elements_by_tag_name('a'):   # todo consider making this part more efficient
            link = element.get_attribute('href')
            if '/p/' in link: yield link


def get_data(proxy, url):   #  todo this function need refactoring - use selenium get tag + encapsulate as class method

    try:
        address = proxy.get_address()
        DesiredCapabilities.CHROME['proxy'] = {
            "httpProxy": address,
            "ftpProxy": address,
            "sslProxy": address,
            "proxyType": "MANUAL",
        }
        chrome_options = Options()
        chrome_options.add_argument(CHROME_HEADLESS)
        driver = Chrome(options=chrome_options)
        driver.get(url)
        data = bs(driver.page_source, 'html.parser')
        body = data.find('body')
        script = body.find('script', text=lambda t: t.startswith(URL_KEY_WORD))
        page_json = script.string.split(URL_KEY_WORD, 1)[-1].rstrip(';')
        json_data = json.loads(page_json)
        posts = json_data['entry_data']['PostPage'][0]['graphql']
        posts = json.dumps(posts)
        posts = json.loads(posts)
        x = pd.DataFrame.from_dict(json_normalize(posts), orient='columns')
        x.columns = x.columns.str.replace('shortcode_media.', '')
    except:
        x = pd.DataFrame()
    return x

def main():
    pickled_results = '/home/yoav/PycharmProjects/ITC/Project#1/data.pkl'
    result = pd.DataFrame()
    req_proxy = RequestProxy()  # you may get different number of proxy when  you run this at each time
    proxies = req_proxy.get_proxy_list()  #  free proxy list
    scraper = HashTagPage(HASHTAG_CORONA_URL, scroll_pause_time_range=(3, 5))
    #pool_lst = []
    for url in scraper.all_urls():
        result.append(get_data(choice(proxies), url))
        print(result)
    result.to_pickle(pickled_results)


if __name__ == '__main__':
    main()


"""
        # This starts the scrolling by passing the driver and a timeout
        # Once scroll returns bs4 parsers the page_source
        i=0
"""