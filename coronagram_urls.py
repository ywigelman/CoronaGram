from bs4 import BeautifulSoup as bs
import time
import json
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from selenium.webdriver import Chrome, DesiredCapabilities
from random import randint, choice
import pickle
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
from multiprocessing import Pool
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
from copy import deepcopy
from cached_property import cached_property
from selenium.webdriver.support.ui import WebDriverWait


#  todo consider putting the project inside a docker with a version of chrome or other browser and matching key

HASHTAG_CORONA_URL = 'https://www.instagram.com/explore/tags/corona/?h__a=1'  # hashtag corona page
POST_URL_PREFIX = 'https://www.instagram.com/p'


class ChromeScraper(object):
    CHROME_HEADLESS = '--headless'  # java scrip command for making a headless browser (no gui)
    SCROLL_HEIGHT = 'return document.body.scrollHeight'  # java scrip command for getting scroll height

    def __init__(self, url: str, headless_mode, implicit_wait, limit: int = np.inf, max_load_time: int = 5):
        self._url = url
        self._headless = headless_mode
        self._implicit_wait = implicit_wait
        self._limit = limit
        self._scrap_lst = []
        self._chrome_options = Options()
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

    @property
    def last_item(self):
        return self._scrap_lst[-1]

    @property
    def items_scraped(self) -> int:
        return len(self._scrap_lst)

    @cached_property
    def driver(self):
        if self._headless:
            self._chrome_options.add_argument(ChromeScraper.CHROME_HEADLESS)
        driver = Chrome(options=self._chrome_options)
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

    def __init__(self, url: str, headless_mode: bool = True, implicit_wait: int = 30,
                 scroll_pause_time_range: tuple = (10, 30), previous_session_last_url_scraped: str = None,
                 limit: int = np.inf):

        self._scroll_pause_time_range = scroll_pause_time_range  # random wait before scroll down
        self._headless = headless_mode
        self._implicit_wait = implicit_wait
        self._last_url_scraped = previous_session_last_url_scraped
        self.url_lst = []
        self.scrolls = 0

        super().__init__(url, headless_mode=self._headless, implicit_wait=self._implicit_wait, limit=limit)

    def scroll(self):  # todo check which exception one gets when reaching the bottom page and use try except
        previous_height = deepcopy(self.page_height)
        self.driver.execute_script(HashTagPage.SCROLL_2_BOTTOM)  # Scroll to the bottom of the page
        time.sleep(randint(*self._scroll_pause_time_range))  # wait between each scroll
        if self.page_height == previous_height:  # if previous height is equal to new height
            return False
        else:
            return True

    def all_urls(self):
        """
        generator method is used for scraping data from pages with infinite scrolling
        :return:
        """
        self.open()
        last_item_ind = self.items_scraped
        while True:
            self.url_page_scrap()
            yield self._scrap_lst[last_item_ind:]
            if not self.scroll():
                return
            last_item_ind = deepcopy(self.items_scraped)

    def url_page_scrap(self):
        """
        generator method for scraping urls from instagram hashtag pages
        :return:
        """
        for element in self.driver.find_elements_by_tag_name('a'):  # todo consider making this part more efficient
            link = element.get_attribute('href')
            if '/p/' in link:  # it seems that in hashtag pages that are posts that are reused
                self._scrap_lst.append(link)
            if self.items_scraped >= self._limit:
                break


class Post(ChromeScraper):
    POST_KEY_WORD = 'window._sharedData = '
    READY_STATE = 'return document.readyState;'

    def __init__(self, url: str, headless_mode, implicit_wait):
        super().__init__(url, headless_mode, implicit_wait)

    def scrap(self):
        self.open()
        WebDriverWait(self.driver, 5).until(
            lambda driver: driver.execute_script('return document.readyState') == 'complete')
        try:
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
    pickled_results = '/home/yoav/PycharmProjects/ITC/Project#1/data.pkl'
    # req_proxy = RequestProxy()  # you may get different number of proxy when  you run this at each time
    # proxies = req_proxy.get_proxy_list()  #  free proxy list

    headless = True
    implicit_wait = 50
    batch_wait_range = (3, 5)
    scroll_pause_time_range = (3, 5)
    cpu = 4
    item_limit = 2000

    scraper = HashTagPage(HASHTAG_CORONA_URL, headless_mode=headless, scroll_pause_time_range=scroll_pause_time_range,
                          limit=item_limit, implicit_wait=implicit_wait)
    pool_lst = []
    pd_record = []
    counter = 0
    for url_batch in scraper.all_urls():
        for url in url_batch:
            counter += 1
            pool_lst.append((url, headless, implicit_wait))
        with Pool(processes=cpu) as p:
            pd_record.extend(p.starmap(launcher, pool_lst))
        time.sleep(randint(*batch_wait_range))
        print(scraper.items_scraped)

    concatenated = pd.concat(pd_record)
    print(concatenated)
    concatenated.to_pickle(pickled_results)


'''
        try:
            driver2 = Chrome(options=chrome_options2)
            driver2.get(url)
            data = bs(driver2.page_source, 'html.parser')
            driver2.close()
            body = data.find('body')
            script = body.find('script', text=lambda t: t.startswith(URL_KEY_WORD))
            page_json = script.string.split(URL_KEY_WORD, 1)[-1].rstrip(';')
            json_data = json.loads(page_json)
            posts = json_data['entry_data']['PostPage'][0]['graphql']
            posts = json.dumps(posts)
            posts = json.loads(posts)
            x = pd.DataFrame.from_dict(json_normalize(posts), orient='columns')
            x.columns = x.columns.str.replace('shortcode_media.', '')
            result = result.append(x.copy())
            if len(result) % 100 == 0:
                print(len(result))
                time.sleep(randint(3, 5))
        except:
           continue

    result.to_pickle(pickled_results)
'''

if __name__ == '__main__':
    main()

"""
        # This starts the scrolling by passing the driver and a timeout
        # Once scroll returns bs4 parsers the page_source
        i=0
"""

# <button class="sqdOP yWX7d     _8A5w5    " type="button"><span>120</span> likes</button>
