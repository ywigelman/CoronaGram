import argparse
import json
import time
from copy import copy
from pathlib import Path
from random import choice
from typing import Union
import pandas as pd
import regex as re
from json.decoder import JSONDecodeError
import selenium.webdriver as wd
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from conf import *
from db_control import DBControl


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
        if additional:
            self._message += '\n' + additional
        super().__init__(self._message)


class Driver(object):
    WEBDRIVER_BROWSERS = {'CHROME': {DRIVER_KEY: wd.Chrome,
                                     OPTIONS_KEY: wd.chrome.options.Options},
                          'FIREFOX': {DRIVER_KEY: wd.Firefox,
                                      OPTIONS_KEY: wd.FirefoxOptions}}

    def __init__(self, user_name: str, password: str, browser: str, implicit_wait: int = DEFAULT_IMPLICIT_WAIT,
                 executable: Union[str, Path, None] = None, *options):
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
        self._user_name = user_name
        self._password = password
        self._browser = browser
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
        if self._browser not in self.WEBDRIVER_BROWSERS:
            raise ClassAttributeError(self._browser, Driver.__class__.__name__, 'browser')
        browser_dict = self.WEBDRIVER_BROWSERS[self._browser]
        return browser_dict[DRIVER_KEY], browser_dict[OPTIONS_KEY]

    def _validate_implicit_wait(self) -> None:
        """
        class function that validates that the implicit wait attribute in a natural number
        :return: None
        """
        try:
            self._implicit_wait = int(self._implicit_wait)
        except (ValueError, TypeError):
            raise ClassAttributeError(self._implicit_wait, self.__name__, 'implicit_wait',
                                      'Implicit wait must be an integer')
        if np.isnan(np.sqrt(self._implicit_wait)):
            raise ClassAttributeError(self._implicit_wait, self.__name__, 'implicit_wait',
                                      'Implicit can not hold negative values')

    def _set_options(self, options: list) -> None:
        """
        a class method for setting web driver option object
        :param options: list object that represent user requirements to be used as driver options
        """
        self._options = self._options()
        # options.append(HEADLESS_MODE)  # adding headless mode as default
        for option in set(options):
            # using "set" in case the same option was added more than once
            try:
                self._options.add_argument(option)
            except ValueError:
                print('note that you have chosen None as an option for your browser.\nthis request will be ignored')

    def _set_driver(self) -> None:
        """
        a class method for setting web driving including all user selected options
        :return: None
        """
        if self._executable is None:  # in cases where none is given it is assumed that the driver path was
            # already added to environment path of the OS
            self._driver = self._driver(options=self._options)
            return
        try:
            self._driver = self._driver(executable_path=str(Path(self._executable).resolve()), options=self._options)
        except (WebDriverException, NotADirectoryError):
            raise ClassAttributeError(self._executable, self.__class__.__name__, 'executable')

    @property
    def driver(self) -> wd:
        """
        property method to get driver
        :return:  selenium webdriver object
        """
        return self._driver

    def login(self):
        """
        a method for opening an instagram url webpage. first by logging in, than accessing the required page
        :return: None
        """
        self.driver.get(ACCOUNT_LOG_IN)
        time.sleep(LOGIN_PAGE_WAIT)
        self.driver.find_element_by_xpath(INSTAGRAM_USER_NAME_TAG).send_keys(self._user_name)
        self.driver.find_element_by_xpath(INSTAGRAM_PASSWORD_TAG).send_keys(self._password)
        self.driver.find_element_by_xpath(INSTAGRAM_SUBMIT_TAG).click()
        time.sleep(LOGIN_PAGE_WAIT)
        self._logged_in = True

    def _not_now(self):
        self.driver.find_element_by_xpath(INSTAGRAM_NOT_NOW_BUTTON).click()
        time.sleep(LOGIN_PAGE_WAIT)
        self.driver.find_element_by_xpath(INSTAGRAM_NOT_NOW_BUTTON).click()
        time.sleep(LOGIN_PAGE_WAIT)

    def open(self, url) -> None:
        if not self._logged_in:
            self.login()
        try:
            self._not_now()
        except NoSuchElementException:
            pass
        self.driver.get(url)


class HashTagPage(object):

    def __init__(self, hashtag: str, driver: Driver,
                 max_scroll_wait: int = DEFAULT_MAX_WAIT_AFTER_SCROLL,
                 min_scroll_wait: int = DEFAULT_MIN_WAIT_AFTER_SCROLL, from_code: Union[str, None] = DEFAULT_FROM_CODE,
                 stop_code: Union[str, None] = DEFAULT_STOP_CODE, limit=DEFAULT_LIMIT):
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
        self._driver_obj.open(HASHTAG_URL_TEMPLATE.format(str(self._hashtag)))
        if self._from_code:
            while True:
                if self._stop_scrapping:
                    print(END_OF_SHORT_CODE_SCAPE_MSG)
                    return
                elif self._break:
                    break
                self._keep_scrolling()
        while True:
            if self._stop_scrapping:
                print(END_OF_SHORT_CODE_SCAPE_MSG)
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
                # todo checks if reaching either stop code or limit
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


class MultiScraper(object):

    def __init__(self, driver):
        self._driver = driver

    @staticmethod
    def _get_hashtags(text):
        """
        :param text:
        :return:
        """
        p = re.compile(r'#(\w*)')
        return p.findall(str(text))

    def _post_scraping(self, shortcode_lst):
        """
        :param shortcode_lst:
        :return:
        """
        record_lst = []
        for shortcode in shortcode_lst:
            try:
                url = WEBSITE_URL + 'p/' + shortcode + '/?__a=1'
                self._driver.driver.get(url)
                record = json.loads(BeautifulSoup(self._driver.driver.page_source, 'html.parser').find('body').get_text())
                record = pd.json_normalize(record)
                record.rename(columns=COL_NAME_DICT, inplace=True)
                record['hashtag'] = record['post_text'].apply(self._get_hashtags)
                record_lst.append(record)
            except (KeyError, JSONDecodeError):
                continue
        return record_lst

    def multiprocess_scraper(self, batch_size: int):
        """
        :param batch_size:
        :param cpu:
        :return:
        """
        dbc = DBControl()
        while True:
            batch = dbc.shortcodes_list_for_scraping(batch_size)
            if not batch:
                return
            records = self._post_scraping(batch)
            pass
            # todo Yair, add a line that checks if enough records to commit and commit


def arg_parser():
    parser = argparse.ArgumentParser(prog='coronagram.py', description=f'#### Instagram Scrapping ####\n',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('tag', type=str, help='Choose a #hashtag')
    parser.add_argument('name', type=str, help='instagram user name')
    parser.add_argument('password', type=str, help='instagram user password')
    parser.add_argument('-l', '--limit', type=int, default=np.inf, help='number of posts to scrape')
    parser.add_argument('-b', '--browser', type=str, default='CHROME', help='browser choice to be used by selenium. '
                                                                            'supported browsers:\t{}'
                        .format('|'.join(Driver.WEBDRIVER_BROWSERS.keys())))
    parser.add_argument('-e', '--executable', type=str, default=None, help='a path to the driver executable file. '
                                                                           'If none is given it will be assumed that '
                                                                           'the driver was added and available as an '
                                                                           'OS environment variable')
    parser.add_argument('-d', '--db_batch', type=int, default=50,
                        help='maximum number of records to insert and commit each time')
    parser.add_argument('-fc', '--from_code', type=str, help='url shortcode to start scraping from')
    parser.add_argument('-sc', '--stop_code', type=str, help='url shortcode that when reach will stop scrapping')
    parser.add_argument('-i', '--implicit_wait', type=int, default=50, help='implicit wait time for '
                                                                                       'webdriver')
    # test that validate that this value is a non negative int
    parser.add_argument('-do', '--driver_options', type=str, default=[], help='ava script optional arguments that will '
                                                                              'be injected to the browser argument '
                                                                              'with selenium webdriver API',
                        action='append')
    parser.add_argument('-mn', '--min_scroll_wait', type=int, default=3,
                        help='minimum number of seconds to wait after each scroll')
    parser.add_argument('-mx', '--max_scroll_wait', type=int, default=5,
                        help='maximum number of seconds to wait after each scroll')

    args = parser.parse_args()

    return args.tag, args.name, args.password, args.limit, args.browser, args.executable, args.db_batch, args.from_code, \
           args.stop_code, args.implicit_wait, args.driver_options, args.min_scroll_wait, args.max_scroll_wait


def main():

    tag, name, password, limit, browser, executable, db_batch, from_code, stop_code, implicit_wait, driver_options, \
    min_scroll_wait, max_scroll_wait = arg_parser()
    driver_set_up = (name, password, browser, implicit_wait, executable, driver_options)
    driver = Driver(*driver_set_up)
    hashtag_page = HashTagPage(tag, driver, max_scroll_wait, min_scroll_wait, from_code, stop_code,
                               limit)
    hashtag_page.shortcode_batch_generator()
    post_scraper = MultiScraper(driver)
    post_scraper.multiprocess_scraper(db_batch)
    driver.driver.close()


if __name__ == '__main__':
    main()


"test mmm_testing_mmm v~)Mav2gTMLBaT) -l 50"