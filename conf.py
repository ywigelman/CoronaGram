import numpy as np

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

WEBSITE_URL = 'https://www.instagram.com/'
ACCOUNT_LOG_IN = WEBSITE_URL + 'accounts/login/'
HASHTAG_URL_TEMPLATE = WEBSITE_URL + 'explore/tags/{}/?h__a=1'

INSTAGRAM_USER_NAME_TAG = "//input[@name=\"username\"]"
INSTAGRAM_PASSWORD_TAG = "//input[@name=\"password\"]"
INSTAGRAM_SUBMIT_TAG = "//button[@type=\"submit\"]"
INSTAGRAM_NOT_NOW_BUTTON = "//button[text()='Not Now']"
LOGIN_PAGE_WAIT = 3

STEP_SIZE = 0.1
DEFAULT_MAX_WAIT_AFTER_SCROLL = 3
DEFAULT_MIN_WAIT_AFTER_SCROLL = 1
DEFAULT_LIMIT = np.inf
DEFAULT_FROM_CODE = None
DEFAULT_STOP_CODE = None

SCROLL_2_BOTTOM = 'window.scrollTo(0, document.body.scrollHeight);'  # java scrip command for scrolling to page bottom
SCROLL_HEIGHT = 'return document.body.scrollHeight'  # java scrip command for getting scroll height
POST_KEY_WORD = 'window._sharedData = '
HEADLESS_MODE = '--headless'  # command for running in headless mode

DEFAULT_IMPLICIT_WAIT = 5
DRIVER_KEY, OPTIONS_KEY = 'DRIVER', 'OPTIONS'

END_OF_SHORT_CODE_SCAPE_MSG = 'scraping is done - either you scraped everything, reached your limit or something ' \
                              'went wrong.\n(lets hope it\'s the first one)'

POST_LENGTH_TO_COMMIT = 2