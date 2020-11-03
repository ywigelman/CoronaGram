from bs4 import BeautifulSoup as bs
import time
import json
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from selenium.webdriver import Chrome, DesiredCapabilities
from random import randint, choice
import pickle
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
from multiprocessing import Pool, cpu_count
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
from copy import deepcopy
from cached_property import cached_property
from selenium.webdriver.support.ui import WebDriverWait


#  todo consider putting the project inside a docker with a version of chrome or other browser and matching key
# constants

HASHTAG_CORONA_URL = 'https://www.instagram.com/explore/tags/corona/?h__a=1'  # hashtag corona page
POST_URL_PREFIX = 'https://www.instagram.com/p'
HEAD_LESS_MODE = True
IMPLICIT_WAIT = 30


class ChromeScraper(object):
    CHROME_HEADLESS = '--headless'  # java scrip command for making a headless browser (no gui)
    SCROLL_HEIGHT = 'return document.body.scrollHeight'  # java scrip command for getting scroll height

    def __init__(self, url: str, headless_mode, implicit_wait, limit: int = np.inf, max_load_time: int = 5):
        self._url = url
        self._headless = headless_mode
        self._implicit_wait = implicit_wait
        self._limit = limit
        self._max_load_time = max_load_time

    def __repr__(self):
        return 'Scraper that uses selenium with google chrome webdriver'

    @property
    def page_height(self):
        return self.driver.execute_script(ChromeScraper.SCROLL_HEIGHT)  # stores the height of the page

    @property
    def body(self):
        """
        a property method for getting the content (body) of an html page
        :return:
        """
        source = self.driver.page_source
        data = bs(source, 'html.parser')
        return data.find('body')

    @cached_property
    def driver(self):
        chrome_options = Options()
        if self._headless:
            chrome_options.add_argument(ChromeScraper.CHROME_HEADLESS)
        driver = Chrome(options=chrome_options)
        driver.implicitly_wait(self._implicit_wait)
        return driver

    def open(self):
        try:
            self.driver.set_page_load_timeout(self._max_load_time)
            self.driver.get(self._url)
        except:   # todo find an exception
            return


class HashTagPage(ChromeScraper):
    SCROLL_2_BOTTOM = 'window.scrollTo(0, document.body.scrollHeight);'  # java scrip command for scrolling to page bottom

    def __init__(self, url: str, headless_mode: bool = HEAD_LESS_MODE, implicit_wait: int = IMPLICIT_WAIT,
                 scroll_pause_time_range: tuple = (10, 30), previous_session_last_url_scraped: str = None,
                 limit: int = np.inf):

        self._scroll_pause_time_range = scroll_pause_time_range  # random wait before scroll down
        self._headless = headless_mode
        self._implicit_wait = implicit_wait
        self._last_url_scraped = previous_session_last_url_scraped
        self._url_batch = []
        self._scraped_urls = 0

        super().__init__(url, headless_mode=self._headless, implicit_wait=self._implicit_wait, limit=limit)

    def scroll(self):  # todo check which exception one gets when reaching the bottom page and use try except
        previous_height = deepcopy(self.page_height)
        self.driver.execute_script(HashTagPage.SCROLL_2_BOTTOM)  # Scroll to the bottom of the page
        time.sleep(randint(*self._scroll_pause_time_range))  # wait between each scroll
        if self.page_height == previous_height:  # if previous height is equal to new height
            return False
        else:
            return True

    def url_batch_gen(self):
        """
        generator method is used for scraping data from pages with infinite scrolling
        :return:
        """
        self.open()
        while True:
            self.url_page_scrap()
            yield self._url_batch
            if not self.scroll():
                return
            self._url_batch = []

    def url_page_scrap(self):
        """
        generator method for scraping urls from instagram hashtag pages
        :return:
        """
        for element in self.driver.find_elements_by_tag_name('a'):  # todo consider making this part more efficient
            link = element.get_attribute('href')
            if '/p/' in link:  # it seems that in hashtag pages that are posts that are reused
                self._url_batch.append(link)
                self._scraped_urls += 1
            if self._scraped_urls >= self._limit:
                break

    def url_scraped(self) -> int:
        return self._scraped_urls


class Post(ChromeScraper):
    POST_KEY_WORD = 'window._sharedData = '
    READY_STATE = 'return document.readyState;'

    def __init__(self, url: str):
        super().__init__(url, HEAD_LESS_MODE, IMPLICIT_WAIT)

    def scrap(self):
        self.open()
        try:
            WebDriverWait(self.driver, 5).until(
                lambda driver: driver.execute_script('return document.readyState') == 'complete')
            script = self.body.find('script', text=lambda t: t.startswith(Post.POST_KEY_WORD))
            page_json, = filter(None, script.string.rstrip(';').split(Post.POST_KEY_WORD, 1))
            posts, = json.loads(page_json)['entry_data']['PostPage']
            posts = posts['graphql']
            return pd.json_normalize(posts)  # todo rename some of the elements
        except Exception:  # todo think about possible exceptions
            return pd.DataFrame()
        finally:
            self.driver.close()


def launcher(*args):
    post = Post(*args)
    try:
        record = post.scrap()
    except WebDriverException:
        return pd.DataFrame()
    return record


def main():
    scroll_pause_time_range = (3, 5)
    cpu = cpu_count()-1  # this will make all your processors work
    print(cpu)
    item_limit = 200

    scraper = HashTagPage(HASHTAG_CORONA_URL, scroll_pause_time_range=scroll_pause_time_range, limit=item_limit)
    pd_record = []
    for url_batch in scraper.url_batch_gen():
        with Pool(processes=cpu) as p: pd_record.extend(p.map(launcher, url_batch))
        if scraper.url_scraped() == item_limit:
            break
        print('done scrapping a total of {} posts so far'.format(scraper.url_scraped()))
    concatenated = pd.concat(pd_record)
    print(concatenated)

    #pickled_results = '/home/yoav/PycharmProjects/ITC/Project#1/data.pkl'
    #concatenated.to_pickle(pickled_results)

if __name__ == '__main__':
    main()

# req_proxy = RequestProxy()  # you may get different number of proxy when  you run this at each time
# proxies = req_proxy.get_proxy_list()  #  free proxy list
