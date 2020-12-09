import numpy as np
from selenium.webdriver.chrome.options import Options
import selenium.webdriver as wd

#     Please Note - in order to put ALL constants, magic numbers, dictionaries, and magic strings in a configuration
#     file as requested in the redo - we had to import packages to this file since since some of those constants are
#     dictionary with selenium commands or values like np.inf

# constants for log file
DEFAULT_LOG_FILE_PATH = 'coronagram.log'
DEFAULT_LOG_FILE_FORMAT = '%(asctime)s-%(levelname)s-FILE:%(filename)s-FUNC:%(funcName)s-LINE:%(lineno)d-%(message)s'

# dictionary for rename post scraped elements
COL_NAME_DICT = {'graphql.shortcode_media.__typename': 'type',
                 'graphql.shortcode_media.id': 'id',
                 'graphql.shortcode_media.shortcode': 'shortcode',
                 'graphql.shortcode_media.dimensions.height': 'dim_height',
                 'graphql.shortcode_media.dimensions.width': 'dim_width',
                 'graphql.shortcode_media.display_url': 'photo_url',
                 'graphql.shortcode_media.accessibility_caption': 'ai_comment',
                 'graphql.shortcode_media.is_video': 'is_video',
                 'graphql.shortcode_media.edge_media_to_tagged_user.edges': 'user_details',
                 'graphql.shortcode_media.edge_media_to_caption.edges': 'post_text',
                 'graphql.shortcode_media.edge_media_to_parent_comment.count': 'comment_count',
                 'graphql.shortcode_media.edge_media_to_parent_comment.edges': 'comments',
                 'graphql.shortcodgraphqle_media.edge_media_preview_comment.count': 'preview_comment_count',
                 'graphql.shortcode_media.edge_media_preview_comment.edges': 'preview_comment',
                 'graphql.shortcode_media.comments_disabled': 'comments_disabled',
                 'graphql.shortcode_media.taken_at_timestamp': 'timestamp',
                 'graphql.shortcode_media.edge_media_preview_like.count': 'like_count',
                 'graphql.shortcode_media.location.id': 'location_id',
                 'graphql.shortcode_media.location.has_public_page': 'location_has_public_page',
                 'graphql.shortcode_media.location.name': 'location_name',
                 'graphql.shortcode_media.location.slug': 'location_slug',
                 'graphql.shortcode_media.location.address_json': 'location_json',
                 'graphql.shortcode_media.owner.id': 'owner_id',
                 'graphql.shortcode_media.owner.is_verified': 'owner_is_verified',
                 'graphql.shortcode_media.owner.profile_pic_url': 'owner_profile_pic_url',
                 'graphql.shortcode_media.owner.username': 'owner_username',
                 'graphql.shortcode_media.owner.full_name': 'owner_full_name',
                 'graphql.shortcode_media.owner.is_private': 'owner_is_private',
                 'graphql.shortcode_media.owner.is_unpublished': 'owner_is_unpublished',
                 'graphql.shortcode_media.owner.pass_tiering_recommendation': 'tiering_recommendation',
                 'graphql.shortcode_media.owner.edge_owner_to_timeline_media.count': 'owner_media_count',
                 'graphql.shortcode_media.owner.edge_followed_by.count': 'owner_edge_followed_by_count',
                 'graphql.shortcode_media.is_ad': 'is_ad',
                 'graphql.shortcode_media.edge_sidecar_to_children.edges': 'multiple_photos',
                 'graphql.shortcode_media.video_duration': 'video_duration',
                 'graphql.shortcode_media.product_type': 'product_type'}

# constants with information on instagram, log in, hashtag and posts pages formats
WEBSITE_URL = 'https://www.instagram.com/'
ACCOUNT_LOG_IN = WEBSITE_URL + 'accounts/login/'
HASHTAG_URL_TEMPLATE = WEBSITE_URL + 'explore/tags/{}/?h__a=1'
POST_URL_TEMPLATE = WEBSITE_URL + 'p/{}/?__a=1'

# constants of commands for logging in instagram page including wait time for response before continuing
INSTAGRAM_USER_NAME_TAG = "//input[@name=\"username\"]"
INSTAGRAM_PASSWORD_TAG = "//input[@name=\"password\"]"
INSTAGRAM_SUBMIT_TAG = "//button[@type=\"submit\"]"
INSTAGRAM_NOT_NOW_BUTTON = "//button[text()='Not Now']"
LOGIN_PAGE_WAIT = 3

# constants with default values for instanting DRIVER object
DEFAULT_IMPLICIT_WAIT = 50
DEFAULT_BROWSER = 'CHROME'
DEFAULT_EXECUTABLE = None
DRIVER_KEY, OPTIONS_KEY = 'DRIVER', 'OPTIONS'
WEBDRIVER_BROWSERS = {'CHROME': {DRIVER_KEY: wd.Chrome,
                                 OPTIONS_KEY: wd.chrome.options.Options},
                      'FIREFOX': {DRIVER_KEY: wd.Firefox,
                                  OPTIONS_KEY: wd.FirefoxOptions}}
HEADLESS_MODE = '--headless'  # command for running in headless mode
DEFAULT_DRIVER_OPTIONS = []
NONE_OPTION_VALUE = 'note that you have chosen None as an option for your browser. this request will be ignored'
IMPLICIT_WAIT_NOT_INT = 'Implicit wait must be an integer'
IMPLICIT_WAIT_NOT_NEG = 'Implicit can not hold negative values'

# constants with default values for instanting HashTagPage object
END_OF_SHORT_CODE_SCAPE_MSG = 'shortcode scraping is done - either you scraped everything, reached your limit or ' \
                              'something went wrong. (lets hope it\'s the first one)'
STEP_SIZE = 0.1
DEFAULT_MAX_WAIT_AFTER_SCROLL = 3
DEFAULT_MIN_WAIT_AFTER_SCROLL = 1
DEFAULT_URL_LIMIT = np.inf
DEFAULT_FROM_CODE = None
DEFAULT_STOP_CODE = None
SCROLL_2_BOTTOM = 'window.scrollTo(0, document.body.scrollHeight);'  # java scrip command for scrolling to page bottom
SCROLL_HEIGHT = 'return document.body.scrollHeight'  # java scrip command for getting scroll height

# constants with default values for instancing PostScraper object
DEFAULT_BATCH_SIZE = 50
DEFAULT_POST_LIMIT = np.inf
POST_LENGTH_TO_COMMIT = 10

# constants for DB connections
DB_NAME = 'itc1'
DB_HOST_NAME = "localhost"
DB_USER_NAME = "root"

# POST_TEXT constants
CONTENT_TYPE = 'application/x-www-form-urlencoded'
ACCEPT_ENCODING = 'application/gzip'
X_RAPID_API_KEY = 'dc8d389c8emsh7b62cf28feb4648p1285cdjsn788a129fffc5'
X_RAPID_API_HOST = 'google-translate1.p.rapidapi.com'
URL_LANGUAGE = 'https://google-translate1.p.rapidapi.com/language/translate/v2/detect'
URL_TRANSLATION = 'https://google-translate1.p.rapidapi.com/language/translate/v2'
-URL_SENTIMENT = 'https://textanalyticsitc.cognitiveservices.azure.com//text/analytics/v3.0/sentiment'
TARGET_LANG = 'en'

# general CLI constants
MAX_ENRICH = 0

