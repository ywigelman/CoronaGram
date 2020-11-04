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
import argparse


#  todo consider putting the project inside a docker with a version of chrome or other browser and matching key
# constants

POST_URL_PREFIX = 'https://www.instagram.com/p'
HEAD_LESS_MODE = True
IMPLICIT_WAIT = 30


DEFAULT_FIELDS = ['id', 'shortcode', 'timestamp', 'photo_url', 'post_text', 'preview_comment', 'ai_comment', 'like_count',
                  'location_name', 'owner_profile_pic_url', 'owner_username', 'owner_full_name',
                  'owner_edge_followed_by_count', 'is_ad']


dic_fields = {
    'type' : '__typename',
    'id' : 'id',
    'shortcode' : 'shortcode',
    'dim_height' : 'dimensions.height',
    'dim_width' : 'dimensions.width',
    'photo_url' : 'display_url',
    'ai_comment' : 'accessibility_caption',
    'is_video' : 'is_video',
    'user_details' : 'edge_media_to_tagged_user.edges',
    'post_text' : 'edge_media_to_caption.edges',
    'comment_count' : 'edge_media_to_parent_comment.count',
    'comments' : 'edge_media_to_parent_comment.edges',
    'edge_media_to_hoisted_comment.edges' : 'edge_media_to_hoisted_comment.edges', ### ????
    'preview_comment_count' : 'edge_media_preview_comment.count',
    'preview_comment' : 'edge_media_preview_comment.edges',
    'comments_disabled' : 'comments_disabled',
    'timestamp' : 'taken_at_timestamp',
    'like_count' : 'edge_media_preview_like.count',
    'location_id' : 'location.id',
    'location_has_public_page' : 'location.has_public_page',
    'location_name' : 'location.name',
    'location_slug' : 'location.slug',
    'location_json' : 'location.address_json',
    'owner_id' : 'owner.id',
    'owner_is_verified' : 'owner.is_verified',
    'owner_profile_pic_url' : 'owner.profile_pic_url',
    'owner_username' : 'owner.username',
    'owner_full_name' : 'owner.full_name',
    'owner_is_private' : 'owner.is_private',
    'owner_is_unpublished' : 'owner.is_unpublished',
    'owner_pass_tiering_recommendation' : 'owner.pass_tiering_recommendation',
    'owner_edge_owner_to_timeline_media_count' : 'owner.edge_owner_to_timeline_media.count',
    'owner_edge_followed_by_count' : 'owner.edge_followed_by.count',
    'is_ad' : 'is_ad'
}



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
    parser = argparse.ArgumentParser(prog='coronagram_urls.py',description=f'#### Instagram Scrapping ####\n',
                                     epilog=f'List of possible fields to choose:\n'
                                            f'{" ".join(list(dic_fields.keys()))}')
    parser.add_argument('tag', type=str, help='Choose a #hashtag')
    parser.add_argument('-n', '--number', type=int, default=1, help='Choose number of posts to print')
    parser.add_argument('-f', '--fields', nargs='*', type=str, help='Choose fields to print. If no -f, default fields '
                                                                    'will be printed. If -f only without fields, all json fields will be'
                                                                    f' printed. Defaults fields are: {" ".join(DEFAULT_FIELDS)}')

    args = parser.parse_args()
    json_fields = []
    if args.fields is None:
        fields = DEFAULT_FIELDS
    else:
        fields = args.fields

    for field in fields:
        json_fields.append(dic_fields[field])

    scroll_pause_time_range = (3, 5)
    cpu = cpu_count() - 1  # this will make all your processors work
    print(cpu)
    item_limit = args.number

    hashtag_url = f'https://www.instagram.com/explore/tags/{args.tag}/?h__a=1'

    scraper = HashTagPage(hashtag_url, scroll_pause_time_range=scroll_pause_time_range, limit=item_limit)
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
