import numpy as np

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

WEBSITE_URL = 'https://www.instagram.com/'
ACCOUNT_LOG_IN = WEBSITE_URL + 'accounts/login/'
HASHTAG_URL_TEMPLATE = WEBSITE_URL + 'explore/tags/{}/?h__a=1'

INSTAGRAM_USER_NAME_TAG = "//input[@name=\"username\"]"
INSTAGRAM_PASSWORD_TAG = "//input[@name=\"password\"]"
INSTAGRAM_SUBMIT_TAG = "//button[@type=\"submit\"]"
INSTAGRAM_NOT_NOW_BUTTON = "//button[text()='Not Now']"

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

DEFAULT_IMPLICIT_WAIT = 50
DRIVER_KEY, OPTIONS_KEY = 'DRIVER', 'OPTIONS'

END_OF_SHORT_CODE_SCAPE_MSG = 'scraping is done - either you scraped everything, reached your limit or something ' \
                              'went wrong.\n(lets hope it\'s the first one)'

