from bs4 import BeautifulSoup as bs
import time
import json
import selenium.webdriver
from selenium.webdriver.chrome.options import Options

from selenium.common.exceptions import WebDriverException
from selenium.webdriver import Chrome
from random import randint, choice
import pickle
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
from multiprocessing import Pool, cpu_count
import pandas as pd
import numpy as np
from copy import deepcopy
from cached_property import cached_property
import argparse
from urllib.request import urlopen

#  todo consider putting the project inside a docker with a version of chrome or other browser and matching key
# constants

POST_URL_PREFIX = 'https://www.instagram.com/p'
HEAD_LESS_MODE = True
IMPLICIT_WAIT = 30
WAIT_COMPLETE = 5
DEFAULT_FIELDS = ['id', 'shortcode', 'timestamp', 'photo_url', 'post_text', 'preview_comment', 'ai_comment',
                  'like_count', 'location_name', 'owner_profile_pic_url', 'owner_username', 'owner_full_name',
                  'owner_edge_followed_by_count', 'is_ad']
COL_NAME_DICT = {'shortcode_media.__typename': 'type',
                 'shortcode_media.id': 'id',
                 'shortcode_media.shortcode': 'shortcode',
                 'shortcode_media.dimensions.height': 'dim_height',
                 'shortcode_media.dimensions.width': 'dim_width',
                 'shortcode_media.display_url': 'photo_url',
                 'shortcode_media.accessibility_caption': 'ai_comment',
                 'shortcode_media.is_video': 'is_video',
                 'shortcode_media.edge_media_to_tagged_user.edges': 'user_details',
                 'shortcode_media.edge_media_to_caption.edges': 'post_text',
                 'shortcode_media.edge_media_to_parent_comment.count': 'comment_count',
                 'shortcode_media.edge_media_to_parent_comment.edges': 'comments',
                 'shortcode_media.edge_media_to_hoisted_comment.edges': 'edge_media_to_hoisted_comment.edges',
                 'shortcode_media.edge_media_preview_comment.count': 'preview_comment_count',
                 'shortcode_media.edge_media_preview_comment.edges': 'preview_comment',
                 'shortcode_media.comments_disabled': 'comments_disabled',
                 'shortcode_media.taken_at_timestamp': 'timestamp',
                 'shortcode_media.edge_media_preview_like.count': 'like_count',
                 'shortcode_media.location.id': 'location_id',
                 'shortcode_media.location.has_public_page': 'location_has_public_page',
                 'shortcode_media.location.name': 'location_name',
                 'shortcode_media.location.slug': 'location_slug',
                 'shortcode_media.location.address_json': 'location_json',
                 'shortcode_media.owner.id': 'owner_id',
                 'shortcode_media.owner.is_verified': 'owner_is_verified',
                 'shortcode_media.owner.profile_pic_url': 'owner_profile_pic_url',
                 'shortcode_media.owner.username': 'owner_username',
                 'shortcode_media.owner.full_name': 'owner_full_name',
                 'shortcode_media.owner.is_private': 'owner_is_private',
                 'shortcode_media.owner.is_unpublished': 'owner_is_unpublished',
                 'shortcode_media.owner.pass_tiering_recommendation': 'owner_pass_tiering_recommendation',
                 'shortcode_media.owner.edge_owner_to_timeline_media.count': 'owner_edge_owner_to_timeline_media_count',
                 'shortcode_media.owner.edge_followed_by.count': 'owner_edge_followed_by_count',
                 'shortcode_media.is_ad': 'is_ad'}
WEBDRIVER_BROWSERS = {
    'CHROME': {
        'DRIVER': selenium.webdriver.Chrome,
        'OPTIONS': selenium.webdriver.chrome.options.Options},
    'FIREFOX': {
        'DRIVER': selenium.webdriver.Firefox,
        'OPTIONS': selenium.webdriver.FirefoxOptions}}


class ChromeScraper(
    object):  # todo remove this class - use driver class instead to initiate driver and other attibutes to HashTagPage class
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
        except:  # todo find an exception
            return


class HashTagPage(ChromeScraper):  # todo add driver class an attribute - composition
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


class Post(object):
    POST_KEY_WORD = 'window._sharedData = '
    READY_STATE = 'return document.readyState;'

    def __init__(self, url: str):
        self._url = url

    def scrap(self):
        try:
            source = urlopen(self._url)
            body = bs(source, 'html.parser').find('body')
            script = body.find('script', text=lambda t: t.startswith(Post.POST_KEY_WORD))
            page_json, = filter(None, script.string.rstrip(';').split(Post.POST_KEY_WORD, 1))
            posts, = json.loads(page_json)['entry_data']['PostPage']
            return posts['graphql']
        except Exception:  # todo think about possible exceptions
            return json.loads('{}')


class Driver(object):  # todo finish driver class to unable setting of either chrom or firefox

    def __init__(self, browser: str, driver: str, headless_mode: bool = HEAD_LESS_MODE,
                 implicit_wait: int = IMPLICIT_WAIT):
        pass


def launcher(*args):
    post = Post(*args)
    try:
        record = post.scrap()
    except WebDriverException:
        return pd.DataFrame()
    return record


def arg_parser():
    parser = argparse.ArgumentParser(prog='coronagram_urls.py', description=f'#### Instagram Scrapping ####\n',
                                     epilog=f'List of possible fields to choose:\n'
                                            f'{" ".join(list(COL_NAME_DICT.values()))}',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('tag', type=str, help='Choose a #hashtag')
    parser.add_argument('-n', '--number', type=int, default=1, help='Choose number of posts to print')
    parser.add_argument('-f', '--fields', nargs='*', type=str, help='Choose fields to print. If no -f, default fields '
                                                                    'will be printed. If -f only without fields, all json fields will be'
                                                                    f' printed. Defaults fields are: {" ".join(DEFAULT_FIELDS)}')
    parser.add_argument('-b', '--browser', type=str, default='chrome',
                        help='browser choice to be used by selenium '
                             'WebDriver. supported browsers:\t{}'.format('|'.join(WEBDRIVER_BROWSERS.keys())))
    parser.add_argument('-c', '--cpu', type=int, default=cpu_count() - 1,
                        help='number of cpu available for multithreading')
    parser.add_argument('-s', '--stop_code', type=str, help='url shortcode of most recent scraped item', default='')
    #  todo add verbosity level for screen prints during scape sessions
    parser.add_argument('-o', '--output', type=str, default=['pkl','insta_output.pkl'], nargs=2, metavar=('method/format',
                        'filename'), help='Choose output file/database. options: csv, pkl, sql')

    return parser.parse_args()


def main():
    args = arg_parser()

    cpu = args.cpu
    output_method = args.output[0]
    output_filename = args.output[1]
    browser = args.browser  # todo browser will be used to set a Driver class
    stop_code = args.stop_code  # todo add a condition in HashTagPage class to stop once reaching this post
    if cpu < 0: cpu = 1

    json_fields = []
    if args.fields is None:
        fields = DEFAULT_FIELDS
    else:
        fields = args.fields

    for field in fields:
        json_fields.append(field)

    scroll_pause_time_range = (3, 5)
    item_limit = args.number

    hashtag_url = f'https://www.instagram.com/explore/tags/{args.tag}/?h__a=1'

    scraper = HashTagPage(hashtag_url, scroll_pause_time_range=scroll_pause_time_range, limit=item_limit)
    json_records = []
    try:
        for url_batch in scraper.url_batch_gen():
            with Pool(processes=cpu) as p:
                json_records.extend(p.map(launcher, url_batch))
            if scraper.url_scraped() == item_limit:
                break
            print('done scrapping a total of {} posts so far'.format(scraper.url_scraped()))
    except Exception as e:
        print('an unexpected error occurred\n{}'.format(e))
    finally:
        pandas_records = [pd.json_normalize(json_record) for json_record in json_records]
        concatenated = pd.concat(pandas_records).reset_index(drop=True).rename(columns=COL_NAME_DICT).loc[:, json_fields]
        print(concatenated)

    if output_method == 'csv':
        pass
        compression_opts = dict(method='zip', archive_name='out.csv')
        pandas_records.to_csv(output_filename, index=False, compression=compression_opts)
    elif output_method == 'pkl':
        concatenated.to_pickle(output_filename)
    elif output_method == 'sql':
        pass


if __name__ == '__main__':
    main()

# req_proxy = RequestProxy()  # you may get different number of proxy when  you run this at each time
# proxies = req_proxy.get_proxy_list()  #  free proxy list
