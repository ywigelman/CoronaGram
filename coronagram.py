from bs4 import BeautifulSoup as bs
import time
import json
import selenium.webdriver
from selenium.common.exceptions import WebDriverException
from random import choice
from multiprocessing import Pool, cpu_count
import pandas as pd
import numpy as np
from copy import deepcopy
import argparse
from urllib.request import urlopen
import regex as re
import sys
from pathlib import Path
from typing import Union

# constants

HEADLESS_MODE = '--headless'
IMPLICIT_WAIT = 30
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
                 'shortcode_media.owner.pass_tiering_recommendation': 'tiering_recommendation',
                 'shortcode_media.owner.edge_owner_to_timeline_media.count': 'owner_media_count',
                 'shortcode_media.owner.edge_followed_by.count': 'owner_edge_followed_by_count',
                 'shortcode_media.is_ad': 'is_ad',
                 'shortcode_media.edge_sidecar_to_children.edges': 'multiple_photos',
                 'shortcode_media.video_duration': 'video_duration',
                 'shortcode_media.product_type': 'product_type'}
DRIVER_KEY, OPTIONS_KEY = 'DRIVER', 'OPTIONS'
WEBDRIVER_BROWSERS = {'CHROME': {DRIVER_KEY: selenium.webdriver.Chrome,
                                 OPTIONS_KEY: selenium.webdriver.chrome.options.Options},
                      'FIREFOX': {DRIVER_KEY: selenium.webdriver.Firefox,
                                  OPTIONS_KEY: selenium.webdriver.FirefoxOptions}}


class Driver(object):
    DEFAULT_IMPLICIT_WAIT = 50

    def __init__(self, browser: str, implicit_wait: int = DEFAULT_IMPLICIT_WAIT, executable: Union[str, Path, None] = None,
                 *options):
        """
        Driver is an object for generating and setting selenium webdriver object with more friendly API and some
        limited options that are suitable for the task of url scrapping from instagram hashtag web pages
        :param browser: str object that represents the type of browser to use
        :param implicit_wait: an int object that represents an implicit wait time (in seconds) for the driver to
        load elements (DOM) in the required web page
        :param executable: a str, Path or None that represents a path to the driver executable file. If None It will
        be assumed that a driver was added as an OS environment variable
        :param options: represents a variable number of java script optional arguments that will be injected to the
        browser argument with selenium webdriver API
        """
        self._browser = browser
        self._executable = executable
        self._implicit_wait = implicit_wait
        self._options = set(*options)  # using "set" in case the same option was added more than once
        self._driver = None

    def set(self) -> None:
        """
        a method to validate instance attributes than generate a selenium web driver object
        :return: None
        """
        while True:  # checking that the selected browser is supported
            browser_dict = WEBDRIVER_BROWSERS.get(self._browser.upper(), None)
            if browser_dict:
                break
            print('You choose {} as your web browser. Unfortunately your choice of browser is '
                  'unsupported'.format(self._browser))
            proceed()
            self._browser = input('Please choose one of the following '
                                  'browsers :\t{}'.format('|'.join(WEBDRIVER_BROWSERS.keys())))

        browser_obj, options_obj = browser_dict[DRIVER_KEY], browser_dict[OPTIONS_KEY]

        while True:  # validate implicit wait attribute
            try:
                self._implicit_wait = int(self._implicit_wait)
                break
            except (ValueError, TypeError):
                print('Implicit wait must be an integer')
                proceed()
            if self._implicit_wait < 0:
                print('Implicit wait can not have a negative value')

        options_instance = options_obj()
        for option in self._options:
            try:
                options_instance.add_argument(option)
            except ValueError:
                print('note that you have chosen None as an option for your browser.\nthis request will be ignored')

        while True:  # validate that executable web driver is valid and generates a web_driver instance
            if self._executable is None:  # in cases where none is given it is assumed that the driver path was
                # already added to environment path of the OS
                self._driver = browser_obj(options=options_instance)
                break
            try:
                self._driver = browser_obj(executable_path=str(Path(self._executable).resolve()),
                                           options=options_instance)
                break
            except (WebDriverException, NotADirectoryError):
                print('the value you enters as a web driver executable path is invalid')
                proceed()
                self._executable = input('Please enter a path of an executable driver:\t')
        self._driver.implicitly_wait(self._implicit_wait)  # setting browser

    @property
    def driver(self) -> selenium.webdriver:
        """
        property method to get driver
        :return:  selenium webdriver object
        """
        return self._driver


class HashTagPage(object):
    SCROLL_2_BOTTOM = 'window.scrollTo(0, document.body.scrollHeight);'  # java scrip command for scrolling to page bottom
    SCROLL_HEIGHT = 'return document.body.scrollHeight'  # java scrip command for getting scroll height
    HASHTAG_URL_TEMPLATE = 'https://www.instagram.com/explore/tags/{}/?h__a=1'
    STEP_SIZE = 0.1
    DEFAULT_MAX_WAIT_AFTER_SCROLL = 3
    DEFAULT_MIN_WAIT_AFTER_SCROLL = 1
    DEFAULT_LIMIT = np.nan
    DEFAULT_FROM_CODE = None
    DEFAULT_STOP_CODE = None

    def __init__(self, hashtag: str, driver: Driver, max_scroll_wait: Union[float, None] = None,
                 min_scroll_wait: Union[float, None] = None, from_code: str = None, stop_code: str = None,
                 limit=DEFAULT_LIMIT):
        """
        HashTagPage is an object that represents a dynamic instagram hashtag page with infinite scrolls with methods
        of scraping urls from
        :param hashtag: str that represents the hashtag page to open
        :param driver: Driver object to use in order to get hashtag page
        :param max_scroll_wait: float or None that represents maximum wait time after each scroll in hashtag page
        :param min_scroll_wait: float or None that represents minimum wait time after each scroll in hashtag page
        :param from_code: str or None that represents instagram shortcode of posts to start scraping from
        :param stop_code: str or None that represents instagram shortcode of posts to stop scraping once reached
        :param limit: int or np.inf that represents the maximum number of urls to scrape
        """

        self._hashtag = hashtag
        self._driver_obj = driver
        self._driver_obj.set()
        self._max_scroll_wait = max_scroll_wait
        self._min_scroll_wait = min_scroll_wait
        self._limit = limit
        self._from_code = from_code
        self._stop_code = stop_code
        self.attribute_validation()

        self._url_batch = []
        self._scraped_urls = 0
        self._previous_height = None
        self._stop_scrapping = False  # a flag that indicates if scrolling reached bottom of the web page
        self._break = False  # a flag that indicates stop automated scrolling and start collecting
        # (only relevant if from_code was provided)
        self._scroll_pause_time_range = np.arange(min_scroll_wait, max_scroll_wait, HashTagPage.STEP_SIZE)

    def attribute_validation(self):
        """
        a method that validate instance attributes and set default values for null values
        :return:
        """
        your_choice = 'you have chosen "{}" as value for {}.'
        negative_value_err = your_choice + '\ntime can not be negative unless you have a time machine'
        not_numerical_msg = your_choice + '\nthis value can not be converted to a number.'
        wait_msg = '{} number of seconds to wait after each scroll'
        please_choose = 'please choose {} :\t'.format(wait_msg)
        wait_items = {'minimum': self._min_scroll_wait, 'maximum': self._max_scroll_wait}
        while True:
            # setting default values in case user didn't choose any
            if any([pd.isna(self._min_scroll_wait), np.isinf(self._min_scroll_wait)]):
                self._min_scroll_wait = HashTagPage.DEFAULT_MIN_WAIT_AFTER_SCROLL
            if any([pd.isna(self._max_scroll_wait), np.isinf(self._max_scroll_wait)]):
                self._max_scroll_wait = HashTagPage.DEFAULT_MAX_WAIT_AFTER_SCROLL
            for wait_item, wait_value in wait_items.items():
                try:
                    np.sqrt(float(wait_value))
                except ValueError:
                    print(not_numerical_msg.format(wait_value, wait_msg.format(wait_item)))
                    proceed()
                    wait_value = input(please_choose.format(wait_item))
                    if wait_item == 'maximum':
                        self._max_scroll_wait = wait_value
                    else:
                        self._min_scroll_wait = wait_value
                    continue
                except RuntimeWarning:
                    print(negative_value_err.format(wait_value, wait_msg.format(wait_item)))
                    proceed()
                    wait_value = input(please_choose.format(wait_item))
                    if wait_item == 'maximum':
                        self._max_scroll_wait = wait_value
                    else:
                        self._min_scroll_wait = wait_value
                    continue
            if self._max_scroll_wait < self._min_scroll_wait:
                print(your_choice.format(self._max_scroll_wait, wait_msg.format('maximum')) +
                      ' and {} as the minimum'.format(self._min_scroll_wait))
                print('maximum wait time can not be smaller than the minimum.')
                proceed()
                self._min_scroll_wait = input(please_choose.format('maximum') +
                                              ' make sure that the chosen value '
                                              'is larger than {}:\t'.format(self._min_scroll_wait))
                continue
            if self._min_scroll_wait == self._max_scroll_wait:  # in case min wait time and max wait is the same
                self._max_scroll_wait += HashTagPage.STEP_SIZE
            break

        while True:
            if pd.isna(self._limit):
                self._limit = HashTagPage.DEFAULT_LIMIT
                break
            elif np.isinf(self._limit):
                break
            try:
                np.sqrt(int(self._limit))
            except ValueError:
                print(not_numerical_msg.format(self._limit, 'number of urls to scrape from the hashtag page'))
                proceed()
                self._limit = input('please provide a number of urls to scrape from an hashtag page:\t')
                continue
            except RuntimeWarning:
                print(negative_value_err.format(self._limit, 'number of urls to scrape from the hashtag page'))
                proceed()
                self._limit = input('please provide a number of urls to scrape from an hashtag page:\t')
                continue
            else:
                break

        if not self._stop_code:
            self._stop_code = HashTagPage.DEFAULT_STOP_CODE

        if not self._from_code:
            self._from_code = HashTagPage.DEFAULT_FROM_CODE

    def scroll(self):
        """
        a method for scrolling down a web page using selenium webdriver after. After each scroll page height will be
        calculated and compared to that of the previous page
        In case their value a different the function will return True, otherwise False. After each scroll the system will
        undergo a random wait period in a given range of seconds that will allow the driver to load new information
        and avoid detection (hopefully).
        :return: bool expression that will indicate if the scroll was successful
        """
        self._driver_obj.driver.execute_script(HashTagPage.SCROLL_2_BOTTOM)  # Scroll to the bottom of the page
        time.sleep(choice(self._scroll_pause_time_range))  # random wait between each scroll
        if self.page_height == self._previous_height:  # if previous height is equal to new height
            self._stop_scrapping = True
        else:
            self._previous_height = deepcopy(self.page_height)

    def url_batch_gen(self) -> list:
        """
        generator method is used for scraping data from pages with infinite scrolling. this method works with
        the following steps:
                                0) zero stage - will keep scrolling until reaching from_code (if from code was provided)
                                1) checks if done scraping by checking if bottom of page has been reached for the
                                second time.
                                2) collects all urls from current page scroll state using the url_page_scrap method
                                3) yield a batch (list) of all urls collected from the current scroll state
                                4) reset/empty url batch container to prepare for another round
                                5) perform another scroll
        :return: list object in every round
        """

        end_of_scrape_msg = 'scraping is done - either you scraped everything, ' \
                            'reached your limit or something went wrong.\n(lets hope it\'s the first one)'
        self.open()
        if self._from_code:
            while True:
                if self._stop_scrapping:
                    print(end_of_scrape_msg)
                    self.close()
                    return
                elif self._break:
                    break
                self.keep_scrolling()

        while True:
            if self._stop_scrapping:
                print(end_of_scrape_msg)
                self.close()
                return
            self.url_page_scrap()
            yield self._url_batch
            self._url_batch = []
            self.scroll()

    def get_links(self) -> filter:
        """
        a method for scraping urls from the current scroll state of an instagram hashtag page and filtering for post
        urls
        :return: filter
        """
        all_links = [element.get_attribute('href') for element in
                     self._driver_obj.driver.find_elements_by_tag_name('a')]
        return filter(lambda link: '/p/' in link, all_links)

    def url_page_scrap(self) -> None:
        """
        a method for loading a batch of urls collected with 'get_links' method and checking if reached limit of
        scraping limit of stop code
        :return: None
        """
        for link in self.get_links():
            if any([str(self._stop_code) in link, self.scraped_urls >= self._limit]):
                # checks if reaching either stop code or limit
                self._stop_scrapping = True
                return
            self._url_batch.append(link)
            self._scraped_urls += 1

    def keep_scrolling(self) -> None:
        """
        a method for checking if urls contains from_code by iteration. if from code was found all urls following this
        urls in the current scroll page are loaded onto url_batch and breaks non-collecting url cycle
        :return:
        """
        links = list(self.get_links())
        for link_ind, link in enumerate(links):
            if str(self._from_code) in link:
                left_overs = links[link_ind + 1:]
                self._url_batch.extend(left_overs)
                self._scraped_urls += len(left_overs)
                self._break = True
                return
        self.scroll()

    @property
    def scraped_urls(self) -> int:
        """
        a property method that returns the number of scrapped urls from an hashtag page
        :return: int object with the number of scraped urls
        """
        return self._scraped_urls

    @property
    def page_height(self):
        """
        property method that returns the current webpage height
        :return: int object that represents the page current height of the webpage
        """
        return self._driver_obj.driver.execute_script(HashTagPage.SCROLL_HEIGHT)

    def open(self) -> None:
        """
        a method for opening an hashtag webpage
        :return: None
        """
        self._driver_obj.driver.get(HashTagPage.HASHTAG_URL_TEMPLATE.format(str(self._hashtag)))

    def close(self):
        """
        method for closing a driver at the end of the scrapping session
        :return:
        """
        self._driver_obj.driver.close()


# constant for proceed function:
SYS_EXIT_MSG = 'would you like to continue Y/n'


def proceed() -> None:
    """
    a function that asks the user if he'd like to proceed with the process
    :return: None
    """
    while True:
        response = input(SYS_EXIT_MSG).lower()  # make the response case insensitive
        if response == 'y':
            return
        elif response == 'n':
            sys.exit('thank you and smell you later')
        else:
            print('your input is invalid')


# constants for post scraping function:
POST_KEY_WORD = 'window._sharedData = '


def post_scraping(url: str):
    try:
        source = urlopen(url)
        body = bs(source, 'html.parser').find('body')
        script = body.find('script', text=lambda t: t.startswith(POST_KEY_WORD))
        page_json, = filter(None, script.string.rstrip(';').split(POST_KEY_WORD, 1))
        posts, = json.loads(page_json)['entry_data']['PostPage']
        return posts['graphql']
    except Exception as e:
        print(e)
        return json.loads('{}')


def normalize(json_record: json):
    return pd.json_normalize(json_record)


def multi_scraper(hashtag_page: HashTagPage, available_cpus: int):
    json_records = []
    try:
        for url_batch in hashtag_page.url_batch_gen():
            with Pool(processes=available_cpus) as p:
                json_records.extend(p.map(post_scraping, url_batch))
                print('done scrapping a total of {} posts. so far...'.format(hashtag_page.scraped_urls))
    except Exception as general_error:    # several web related exception
        print('an unexpected error has occurred\n{}'.format(general_error))
    finally:
        with Pool(processes=available_cpus) as normalization:
            pandas_records = normalization.map(normalize, json_records)
        return pd.concat(pandas_records).astype(str).drop_duplicates().reset_index(drop=True)


def get_hashtags(text):
    p = re.compile(r'#(\w*)')
    return p.findall(text)


def arg_parser():
    parser = argparse.ArgumentParser(prog='coronagram.py', description=f'#### Instagram Scrapping ####\n',
                                     epilog=f'List of possible fields to choose:\n'
                                            f'{" ".join(list(COL_NAME_DICT.values()))}',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('tag', type=str, help='Choose a #hashtag')
    parser.add_argument('-l', '--limit', type=int, default=np.inf, help='number of posts to scrape')
    parser.add_argument('-f', '--fields', nargs='*', type=str, help='Choose posts fields to keep. '
                                                                    'If no -f, default fields will be printed. '
                                                                    'If -f only without fields, all json fields will be'
                                                                    f' printed. Defaults fields are:'
                                                                    f' {" ".join(DEFAULT_FIELDS)}')
    parser.add_argument('-b', '--browser', type=str, default='CHROME', help='browser choice to be used by selenium. '
                                                                            'supported browsers:\t{}'
                        .format('|'.join(WEBDRIVER_BROWSERS.keys())))
    parser.add_argument('-e', '--executable', type=str, default=None, help='a path to the driver executable file. '
                                                                           'If none is given it will be assumed that '
                                                                           'the driver was added and available as an '
                                                                           'OS environment variable')
    parser.add_argument('-c', '--cpu', type=int, default=cpu_count() - 1,
                        help='number of cpu available for multiprocessing')
    parser.add_argument('-fc', '--from_code', type=str, help='url shortcode to start scraping from')
    parser.add_argument('-sc', '--stop_code', type=str, help='url shortcode that when reach will stop scrapping')
    parser.add_argument('-i', '--implicit_wait', type=int, default=50, help='implicit wait time for '
                                                                                       'webdriver')
    # test that validate that this value is a non negative int
    parser.add_argument('-do', '--driver_options', type=str, default=[], help='ava script optional arguments that will '
                                                                              'be injected to the browser argument '
                                                                              'with selenium webdriver API',
                        action='append')
    parser.add_argument('-hd', '--headed', default=False, action='store_true',
                        help='run with added mode (with browser gui')
    parser.add_argument('-mn', '--min_scroll_wait', type=int, default=3,
                        help='minimum number of seconds to wait after each scroll')
    parser.add_argument('-mx', '--max_scroll_wait', type=int, default=5,
                        help='maximum number of seconds to wait after each scroll')

    args = parser.parse_args()

    if not args.headed:
        args.driver_options.append(HEADLESS_MODE)

    json_fields = []
    if args.fields is None:
        fields = DEFAULT_FIELDS
    else:
        fields = args.fields

    for field in fields:
        json_fields.append(field)

    if args.cpu <= 0:
        args.cpu = 1

    return args.tag, args.limit, json_fields, args.browser, args.executable, args.cpu, args.from_code, args.stop_code, \
           args.implicit_wait, args.driver_options, args.headed, args.min_scroll_wait, args.max_scroll_wait


def main():
    tag, limit, fields, browser, executable, cpu, from_code, stop_code, implicit_wait, driver_options, headed, \
    min_scroll_wait, max_scroll_wait = arg_parser()
    driver = Driver(browser, implicit_wait, executable, driver_options)
    hashtag_page = HashTagPage(tag, driver, max_scroll_wait, min_scroll_wait, from_code, stop_code, limit)
    records = multi_scraper(hashtag_page, cpu)
    records = records.rename(columns=COL_NAME_DICT).loc[:, fields]
    records['hashtag'] = records['post_text'].apply(get_hashtags)  # adding hashtag tags
    print(records)


if __name__ == '__main__':
    main()
