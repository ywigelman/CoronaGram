from bs4 import BeautifulSoup as bs
import time
import json
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import Chrome
from random import randint
import pickle

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
        self.container = []
        self.pre_scroll_height = self.driver.execute_script(SCROLL_HEIGHT)  # stores the height of the page


class HashTagPage(ChromeScraper):

    def __init__(self, url: str, headless_mode: bool = True, implicit_wait: int = 30,
                 scroll_pause_time_range: tuple = (10, 30)):

        self.scroll_pause_time_range = scroll_pause_time_range
        self.headless = headless_mode
        self.implicit_wait = implicit_wait
        # before scroll down
        super().__init__(url, headless_mode=self.headless, implicit_wait=self.implicit_wait)

    def all_urls(self):
        """
        this method is used for scraping data from pages with infinite scrolling
        :return:
        """

        while True:
            self.driver.execute_script(SCROLL_2_BOTTOM)  # Scroll 2 bottom of the page
            time.sleep(randint(*self.scroll_pause_time_range))  # wait between each scroll
            # Calculate new scroll height and compare with last scroll height
            post_scroll_height = self.driver.execute_script(SCROLL_HEIGHT)
            if post_scroll_height == self.pre_scroll_height:
                # If heights are the same it will exit the function
                break
            self.pre_scroll_height = post_scroll_height
            self.get_urls()
            print(len(self.container))

    def get_urls(self):
        source = self.driver.page_source
        data = bs(source, 'html.parser')  # todo change the bs data collection part as method of supper class
        body = data.find('body')
        script = body.find('script', text=lambda t: t.startswith(URL_KEY_WORD))
        page_json = script.string.split(URL_KEY_WORD, 1)[-1].rstrip(';')
        data = json.loads(page_json)
        for link in data['entry_data']['TagPage'][0]['graphql']['hashtag']['edge_hashtag_to_media']['edges']:  # todo concider multiprocessing and generator  #  todo add stop for last line detected
            self.container.append('/'.join([POST_URL_PREFIX, link['node']['shortcode'], '']))


def main():
    pickled_lst = '/home/yoav/PycharmProjects/ITC/Project#1/url_lst.pkl'
    scraper = HashTagPage(HASHTAG_CORONA_URL, scroll_pause_time_range=(3, 5))
    scraper.all_urls()
    with open(pickled_lst, 'wb'):
        pickle.dumps(scraper.container)


if __name__ == '__main__':
    main()
