import argparse
import json
import time
from copy import copy
from pathlib import Path
from random import choice
from typing import Union
import pandas as pd
import regex as re
from bs4 import BeautifulSoup
from conf import *
from db_control import DBControl
import logging
import sys
from json.decoder import JSONDecodeError
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException


class ClassAttributeError(Exception):
    def __init__(self, wrong_input, class_name: str, attribute_name: str, additional: str = None):
        """
        custom error message that will be raised in cases the user does not use a suitable values for class attributes
        :param wrong_input: the input the user choose
        :param class_name: the class the user tried to set value to
        :param attribute_name: the name of the attribute/variable the user tried to set value to
        :param additional: additional message to the one that will be generated by this class
        """
        self._message = '{} is not a valid input as {} for class {}'.format(wrong_input, attribute_name, class_name)
        logging.error('{}: {}'.format(ClassAttributeError.__name__, self._message))
        if additional:
            self._message += '\n' + additional
        super().__init__(self._message)


class Driver(object):

    def __init__(self, user_name: str, password: str, browser: str, implicit_wait: int = DEFAULT_IMPLICIT_WAIT,
                 executable: Union[str, Path, None] = DEFAULT_EXECUTABLE, *options):
        """
        Driver is an object for generating and setting selenium webdriver object with more friendly API and some
        limited options that are suitable for the task of url scrapping from instagram hashtag web pages
        :param user_name: str object that represents instagram account user name
        :param password: str object that represents instagram account password
        :param browser: str object that represents the type of browser to use
        :param implicit_wait: an int object that represents an implicit wait time (in seconds) for the driver to
        load elements (DOM) in the required web page
        :param executable: a str, Path or None that represents a path to the driver executable file. If None It will
        be assumed that a driver was added as an OS environment variable
        :param options: represents a variable number of java script optional arguments that will be injected to the
        browser argument with selenium webdriver API
        """
        self._user_name = user_name
        self._password = password
        self._browser = browser.upper()
        self._driver, self._options = self._browser_dict()  # selecting suitable driver and option objects
        self._set_options(list(*options))  # setting option object
        self._executable = executable
        self._implicit_wait = implicit_wait
        self._validate_implicit_wait()
        self._set_driver()
        self._driver.implicitly_wait(self._implicit_wait)  # setting browser
        self._logged_in = False

    def _browser_dict(self) -> tuple:
        """
        class property that returns selenium driver and option objects matching to the browser that the
        user have selected
        :return: tuple
        """
        if self._browser not in WEBDRIVER_BROWSERS:
            raise ClassAttributeError(self._browser, Driver.__class__.__name__, 'browser')
        browser_dict = WEBDRIVER_BROWSERS[self._browser]
        logging.info('selected browser: {}'.format(self._browser))
        return browser_dict[DRIVER_KEY], browser_dict[OPTIONS_KEY]

    def _validate_implicit_wait(self) -> None:
        """
        class function that validates that the implicit wait attribute in a natural number
        :return: None
        """
        try:
            self._implicit_wait = int(self._implicit_wait)
            logging.info('implicit_wait value: {}'.format(self._implicit_wait))
        except (ValueError, TypeError):
            raise ClassAttributeError(self._implicit_wait, self.__name__, 'implicit_wait', IMPLICIT_WAIT_NOT_INT)
        if np.isnan(np.sqrt(self._implicit_wait)):
            raise ClassAttributeError(self._implicit_wait, self.__name__, 'implicit_wait', IMPLICIT_WAIT_NOT_NEG)

    def _set_options(self, options: list) -> None:
        """
        a class method for setting web driver option object
        :param options: list object that represent user requirements to be used as driver options
        """
        self._options = self._options()
        self._options.add_experimental_option("prefs", {"intl.accept_languages": "en-EN"})
        for option in set(options): # using "set" in case the same option was added more than once
            try:
                self._options.add_argument(option)
            except ValueError:
                logging.warning(NONE_OPTION_VALUE)
        if self._options:
            logging.info('browser options set successfully')

    def _set_driver(self) -> None:
        """
        a class method for setting web driving including all user selected options
        :return: None
        """
        try:
            if self._executable:
                self._executable = str(Path(self._executable).resolve())
                self._driver = self._driver(executable_path=self._executable, options=self._options)
            else:
                self._driver = self._driver(options=self._options)
        except (WebDriverException, NotADirectoryError):
            raise ClassAttributeError(self._executable, self.__class__.__name__, '{} executable'.format(self._browser))
        logging.info('successfully setting driver object')

    @property
    def driver(self) -> wd:
        """
        property method to get driver
        :return:  selenium webdriver object
        """
        return self._driver

    @property
    def logged_in(self):
        """
        property method that returns the log in status
        :return:
        """
        return self._logged_in

    def login(self):
        """
        a method for logging in instagram
        :return: None
        """
        self.driver.get(ACCOUNT_LOG_IN)
        time.sleep(LOGIN_PAGE_WAIT)
        self.driver.find_element_by_xpath(INSTAGRAM_USER_NAME_TAG).send_keys(self._user_name)
        self.driver.find_element_by_xpath(INSTAGRAM_PASSWORD_TAG).send_keys(self._password)
        self.driver.find_element_by_xpath(INSTAGRAM_SUBMIT_TAG).click()
        time.sleep(LOGIN_PAGE_WAIT)
        source = BeautifulSoup(self.driver.page_source, 'html.parser')
        if source.find('p'):
            logging.info('log in: Failure')
            self.driver.close()
            sys.exit()
        else:
            logging.info('log in: Success')
            self._logged_in = True
            return

    def not_now(self):
        """
        method for avoiding instagram pop ups for saving user log in info and offer for enabling automatic notifications
        :return:
        """
        try:
            self.driver.find_element_by_xpath(INSTAGRAM_NOT_NOW_BUTTON).click()
            time.sleep(LOGIN_PAGE_WAIT)
            logging.info('avoided save contact info message')
            self.driver.find_element_by_xpath(INSTAGRAM_NOT_NOW_BUTTON).click()
            time.sleep(LOGIN_PAGE_WAIT)
            logging.info('avoided Turn on Notifications message')
        except NoSuchElementException:
            logging.warning('tried to avoid contact info message and/or turn on notifications message but couldn\'t '
                            'find a "not now" button')

    def open(self, url: str) -> None:
        """
        a method for opening either hashtag or post url pages
        :param url: string object that represents url page to open
        :return: None
        """
        if not self.logged_in:
            self.login()
            self._logged_in = True
            self.not_now()
        self.driver.get(url)
        logging.info('successfully opened {} url page'.format(url))


class HashTagPage(object):

    def __init__(self, hashtag: str, driver: Driver,
                 max_scroll_wait: int = DEFAULT_MAX_WAIT_AFTER_SCROLL,
                 min_scroll_wait: int = DEFAULT_MIN_WAIT_AFTER_SCROLL, from_code: Union[str, None] = DEFAULT_FROM_CODE,
                 stop_code: Union[str, None] = DEFAULT_STOP_CODE, limit=DEFAULT_URL_LIMIT):
        """
        HashTagPage is an object that represents a dynamic instagram hashtag page with infinite scrolls
        :param hashtag: str that represents the hashtag url page to open
        :param driver: Driver object to use in order to get hashtag page
        :param max_scroll_wait: int or None that represents maximum wait time after each scroll in hashtag page
        :param min_scroll_wait: int or None that represents minimum wait time after each scroll in hashtag page
        :param from_code: str or None that represents instagram shortcode of posts to start scraping from
        :param stop_code: str or None that represents instagram shortcode of posts to stop scraping once reached
        :param limit: int or np.inf that represents the maximum number of shortcodes to scrape
        """
        super().__init__()
        self._hashtag = hashtag
        self._driver_obj = driver
        self._scroll_pause_time_range = self._set_scroll_pause_range(min_scroll_wait, max_scroll_wait)
        self._limit = limit
        self._validate_limit()
        self._from_code, self._stop_code = from_code, stop_code
        self._shortcode_batch, self._scraped_shortcodes = [], 0
        self._previous_height = None
        self._stop_scrapping = False  # a flag that indicates if scrolling reached bottom of the web page
        self._break = False  # a flag that indicates stop automated scrolling and start collecting
        # (only relevant if from_code was provided)
        self._dbc = DBControl()
        logging.info('connection to SQL server for shortcode scraping - successful')

    def _set_scroll_pause_range(self, minimum: int, maximum: int) -> np.ndarray:
        """
        a class method for setting a range for selecting random wait seconds after each scroll
        :param minimum: int object that represents the minimum number of seconds to wait
        :param maximum: int object that represents the maximum number of seconds to wait
        :return: np.ndarray that represents a range of seconds
        """
        if any([not isinstance(minimum, int), minimum < 0]):
            raise ClassAttributeError(minimum, self.__class__.__name__, 'min_scroll_wait', 'min_scroll_wait should be '
                                                                                           'a positive integer')
        if any([not isinstance(minimum, int), maximum <= minimum]):
            raise ClassAttributeError(minimum, self.__class__.__name__,
                                      'max_scroll_wait', 'max_scroll_wait should be a positive integer that is larger '
                                                         'than min_scroll_wait')
        logging.info('set random wait range with minimum value of {}, maximum value of {} and step size of {}'
                     .format(minimum, maximum, STEP_SIZE))
        return np.arange(minimum, maximum, STEP_SIZE)

    def _validate_limit(self) -> None:
        """
        class method that validates that limit attribute is either positive int of np.inf
        :return:
        """
        if not any([np.isinf(self._limit), isinstance(self._limit, int)]):
            raise ClassAttributeError(self._limit, self.__class__.__name__,
                                      'limit', 'limit should be either an integer or np.inf')
        if self._limit < 0:
            raise ClassAttributeError(self._limit, self.__class__.__name__, 'limit', 'limit can not be negative')
        logging.info('set limit value for url scrapping: {}'.format(self._limit))

    def _scroll(self) -> None:
        """
        a method for scrolling down a web page using selenium webdriver after.
        After each scroll page height will be recalculated and compared to that of the previous page
        In case the page height remains the it would be assumed page bottom is reached and stop scrapping flag will be
        set to True. After each scroll the system will undergo a random wait period in a given range of seconds that
        will allow the driver to load new information and avoid detection (hopefully).
        :return: None
        """
        self._driver_obj.driver.execute_script(SCROLL_2_BOTTOM)  # Scroll to the bottom of the page
        time.sleep(choice(self._scroll_pause_time_range))  # random wait between each scroll
        if self.page_height == self._previous_height:  # if previous height is equal to new height
            self._stop_scrapping = True
        else:
            self._previous_height = copy(self.page_height)  # change back to deepcopy if there is an issue

    def shortcode_batch_generator(self):
        """
        generator method is used for scraping data from pages with infinite scrolling. this method works with
        the following steps:
                                0) zero stage - will keep scrolling until reaching from_code (if from code was provided)
                                1) checks if stop scraping flag was raised - if yes returns.
                                2) collects all shortcodes from current page scroll state using the
                                 _shortcode_page_scraper method
                                3) yield a batch (list) of all shortcodes collected from the current scroll state
                                4) reset/empty shortcode batch container to prepare for another round
                                5) perform another scroll
        :return: None
        """
        self._driver_obj.open(HASHTAG_URL_TEMPLATE.format(self._hashtag))
        if self._from_code:
            while True:
                if self._stop_scrapping:
                    logging.info(END_OF_SHORT_CODE_SCAPE_MSG)
                    return
                elif self._break:
                    break
                self._keep_scrolling()
        while True:
            if self._stop_scrapping:
                logging.info(END_OF_SHORT_CODE_SCAPE_MSG)
                return
            self._shortcode_page_scraper()
            self._dbc.insert_shortcodes(self._shortcode_batch)
            self._shortcode_batch = []
            self._scroll()

    def _get_shortcodes(self) -> filter:
        """
        a method for scraping for unique post shortcodes from the current scroll state of an instagram hashtag
        :return: filter
        """
        all_links = [element.get_attribute('href') for element in
                     self._driver_obj.driver.find_elements_by_tag_name('a')]
        return set(Path(url).name for url in filter(lambda link: '/p/' in link, all_links))

    def _shortcode_page_scraper(self) -> None:
        """
        a method for loading a batch of shortcodes collected with '_get_shortcodes' method and checking if
        reached scraping limit of encountered a stop code
        :return: None
        """
        for shortcode in self._get_shortcodes():
            if any([self._stop_code == shortcode, self.scraped_shortcodes >= self._limit]):
                self._stop_scrapping = True
                return
            self._shortcode_batch.append(shortcode)
            self._scraped_shortcodes += 1

    def _keep_scrolling(self) -> None:
        """
        a method for checking if urls contains from_code by iteration. if from code was found all urls following this
        urls in the current scroll page are loaded onto url_batch and breaks non-collecting url cycle
        :return:
        """
        links = list(self._get_shortcodes())
        for link_ind, link in enumerate(links):
            if str(self._from_code) in link:
                left_overs = links[link_ind + 1:]
                self._shortcode_batch.extend(left_overs)
                self._scraped_shortcodes += len(left_overs)
                self._break = True
                return
        self._scroll()

    @property
    def scraped_shortcodes(self) -> int:
        """
        a property method that returns the number of scrapped urls from an hashtag page
        :return: int object with the number of scraped urls
        """
        return self._scraped_shortcodes

    @property
    def page_height(self):
        """
        property method that returns the current webpage height
        :return: int object that represents the page current height of the webpage
        """
        return self._driver_obj.driver.execute_script(SCROLL_HEIGHT)


class PostScraper(object):

    def __init__(self, driver: Driver):
        """
        PostScraper is an object made for scarping instagram pages and insert the results into an SQL database
        :param driver: Driver object with predefine options able to connect to post pages
        """
        self._driver = driver
        if not self._driver.logged_in:
            self._driver.login()
            self._driver.not_now()
        self.posts_scraped = 0

    @staticmethod
    def _get_hashtags(text):
        """
        a static method for extracting hash tags from post
        :param text: an object representing a text containing hashtag to exctract
        :return: list of hashtags
        """
        p = re.compile(r'#(\w*)')
        return p.findall(str(text))

    def _post_scraping(self, shortcode_lst: list) -> list:
        """
        a method for scraping post information from a list of shortcodes
        :param shortcode_lst: a list object that contain post short codes for scrapping
        :return: a list of pandas records containing posts scrape data
        """
        record_lst = []
        for shortcode in shortcode_lst:
            url = POST_URL_TEMPLATE.format(shortcode)
            try:
                self._driver.driver.get(url)
                record = json.loads(BeautifulSoup(self._driver.driver.page_source, 'html.parser')
                                    .find('body').get_text())
                record = pd.json_normalize(record)
                record.rename(columns=COL_NAME_DICT, inplace=True)
                record['hashtags'] = record['post_text'].apply(self._get_hashtags)
                record_lst.append(record)
                logging.info('successfully scraped post - {}'.format(url))
            except (KeyError, JSONDecodeError):
                logging.warning('failed scraping post - {}'.format(url))
        return record_lst

    def scrape(self, batch_size: int = DEFAULT_BATCH_SIZE, max_post_to_scrape: int = DEFAULT_URL_LIMIT):
        """
        a method for scrapping instagram post pages given from a list of unscraped shortcodes in an SQL database.
        this method will work in iterations, each time receiving a batch (with size given as a parameter by the user)
        perform scraping, update the DB and commit
        :param batch_size: int object that represents the size of each batch
        :param max_post_to_scrape: int object that represents an upper limit for the number of posts to scrape
        :return:
        """
        dbc = DBControl()
        records = []
        logging.info('connection to SQL server for post scraping and update step - successful')
        logging.info('set limit value for post scrapping: {}'.format(max_post_to_scrape))
        while True:
            batch = dbc.shortcodes_list_for_scraping(batch_size)
            if any([not batch, self.posts_scraped >= max_post_to_scrape]):
                return
            post_len2add = len(batch) + self.posts_scraped
            if post_len2add > max_post_to_scrape:
                delta = post_len2add - max_post_to_scrape
                batch, return_batch = batch[:delta], batch[delta:]
                dbc.unconfirm_end_scraping_for_shortcodes(return_batch)
            records += self._post_scraping(batch)
            self.posts_scraped += len(batch)
            if len(records) >= POST_LENGTH_TO_COMMIT:
                dbc.insert_posts(records)
                records = []


def arg_parser():
    parser = argparse.ArgumentParser(prog='coronagram.py', description=f'#### Instagram Scrapping ####\n',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('tag', type=str, help='Choose a #hashtag')
    parser.add_argument('name', type=str, help='instagram user name')
    parser.add_argument('password', type=str, help='instagram user password')
    parser.add_argument('-lu', '--url_limit', type=int, default=DEFAULT_URL_LIMIT, help='maximum urls to scrape')
    parser.add_argument('-lp', '--post_limit', type=int, default=DEFAULT_POST_LIMIT, help='maximum posts to scrape')
    parser.add_argument('-b', '--browser', type=str, default=DEFAULT_BROWSER,
                        help='browser choice to be used by selenium. supported browsers:\t{}'
                        .format('|'.join(WEBDRIVER_BROWSERS.keys())))
    parser.add_argument('-e', '--executable', type=str, default=DEFAULT_EXECUTABLE, help='a path to the driver '
                                                                                         'executable file. '
                                                                           'If none is given it will be assumed that '
                                                                           'the driver was added and available as an '
                                                                           'OS environment variable')
    parser.add_argument('-d', '--db_batch', type=int, default=DEFAULT_BATCH_SIZE,
                        help='maximum number of records to insert and commit each time')
    parser.add_argument('-fc', '--from_code', type=str, help='url shortcode to start scraping from',
                        default=DEFAULT_FROM_CODE)
    parser.add_argument('-sc', '--stop_code', type=str, help='url shortcode that when reach will stop scrapping',
                        default=DEFAULT_STOP_CODE)
    parser.add_argument('-i', '--implicit_wait', type=int, default=DEFAULT_IMPLICIT_WAIT, help='implicit wait time for '
                                                                                       'webdriver')
    # test that validate that this value is a non negative int
    parser.add_argument('-o', '--driver_options', type=str, default=DEFAULT_DRIVER_OPTIONS,
                        help='java script optional arguments that will be injected to the browser argument'
                             'with selenium webdriver API', action='append')
    parser.add_argument('-mn', '--min_scroll_wait', type=int, default=DEFAULT_MIN_WAIT_AFTER_SCROLL,
                        help='minimum number of seconds to wait after each scroll')
    parser.add_argument('-mx', '--max_scroll_wait', type=int, default=DEFAULT_MAX_WAIT_AFTER_SCROLL,
                        help='maximum number of seconds to wait after each scroll')
    parser.add_argument('-hd', '--headed_mode', help='running in headed mode (graphical browser)', action='store_true')

    args = parser.parse_args()
    if not args.headed_mode:
        args.driver_options.append(HEADLESS_MODE)

    return args.tag, args.name, args.password, args.url_limit, args.post_limit, args.browser, args.executable, \
           args.db_batch, args.from_code, args.stop_code, args.implicit_wait, args.driver_options, \
           args.min_scroll_wait, args.max_scroll_wait


def main():
    # setting variables
    tag, name, password, url_limit, post_limit, browser, executable, db_batch, from_code, stop_code, implicit_wait, \
    driver_options, min_scroll_wait, max_scroll_wait = arg_parser()
    # setting log file
    logging.basicConfig(filename=DEFAULT_LOG_FILE_PATH, format=DEFAULT_LOG_FILE_FORMAT, level=logging.INFO)
    # scraping urls and posts
    driver_set_up = (name, password, browser, implicit_wait, executable, driver_options)
    driver = Driver(*driver_set_up)
    logging.info('driver object - set')
    HashTagPage(tag, driver, max_scroll_wait, min_scroll_wait, from_code, stop_code, url_limit).\
        shortcode_batch_generator()
    logging.info('done short code scrapping step')
    PostScraper(driver).scrape(db_batch, post_limit)
    driver.driver.close()
    logging.info('done post scraping step')


if __name__ == '__main__':
    main()
