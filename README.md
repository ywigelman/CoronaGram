# CoronaGram
Data Mining Project as part of an ITC Data Science course 

Our program scrap instagram page of a specific hashtag and
take all post link related to this hashtag.
After that it scraps each post page and print the result
as a data table.

The program is optimized to work with different CPUs in 
parallel. Quantity of posts for Corona hashtag for example
is huge, more than 20 millions posts, so optimization
is essential.

It used selenium library for scrolling on page
because instagram has dynamic pages. Selenium headless 
option permits to limit usage of graphical resources.
Selenium library permits to work with the browser installed on
the computer (Chrome or Firefox). In case any browser is defined as default browser,
you have to define a path to it. If it is set, the driver will use the one defined in
OS environment variable


Important arguments user can choose:
1) hashtag to scrap (example: corona)
2) number of CPUs to use (default: all)
3) number of posts to scrap (default: 1)
4) column to print (example: post_url, #like, location...)
5) browser to use (default: chrome)



usage: coronagram.py [-h] [-l LIMIT] [-f [FIELDS ...]] [-b BROWSER] [-e EXECUTABLE] [-c CPU] [-fc FROM_CODE]
                     [-sc STOP_CODE] [-i IMPLICIT_WAIT] [-do DRIVER_OPTIONS] [-hd] [-mn MIN_SCROLL_WAIT]
                     [-mx MAX_SCROLL_WAIT]
                     tag

#### Instagram Scrapping ####

positional arguments:
  tag                   Choose a #hashtag  

optional arguments:  
  -h, --help            show this help message and exit  
  -l LIMIT, --limit LIMIT  
                        number of posts to scrape (default: inf)  
  -f [FIELDS ...], --fields [FIELDS ...]  
                        Choose posts fields to keep. If no -f, default fields will be printed. If -f only  
                        without fields, all json fields will be printed. Defaults fields are: id shortcode  
                        timestamp photo_url post_text preview_comment ai_comment like_count location_name  
                        owner_profile_pic_url owner_username owner_full_name owner_edge_followed_by_count is_ad
                        (default: None)  
  -b BROWSER, --browser BROWSER  
                        browser choice to be used by selenium. supported browsers: CHROME|FIREFOX (default:  
                        CHROME)  
  -e EXECUTABLE, --executable EXECUTABLE  
                        a path to the driver executable file. If none is given it will be assumed that the  
                        driver was added and available as an OS environment variable (default: None)  
  -c CPU, --cpu CPU     number of cpu available for multiprocessing (default: 7)  
  -fc FROM_CODE, --from_code FROM_CODE  
                        url shortcode to start scraping from (default: None)  
  -sc STOP_CODE, --stop_code STOP_CODE  
                        url shortcode that when reach will stop scrapping (default: None)  
  -i IMPLICIT_WAIT, --implicit_wait IMPLICIT_WAIT  
                        implicit wait time for webdriver (default: 30)  
  -do DRIVER_OPTIONS, --driver_options DRIVER_OPTIONS  
                        ava script optional arguments that will be injected to the browser argument with  
                        selenium webdriver API (default: [])  
  -hd, --headed         run with added mode (with browser gui (default: False)  
  -mn MIN_SCROLL_WAIT, --min_scroll_wait MIN_SCROLL_WAIT  
                        minimum number of seconds to wait after each scroll (default: 1)  
  -mx MAX_SCROLL_WAIT, --max_scroll_wait MAX_SCROLL_WAIT  
                        maximum number of seconds to wait after each scroll (default: 3)  

List of possible fields to choose: type id shortcode dim_height dim_width photo_url ai_comment is_video  
user_details post_text comment_count comments preview_comment_count preview_comment comments_disabled timestamp  
like_count location_id location_has_public_page location_name location_slug location_json owner_id  
owner_is_verified owner_profile_pic_url owner_username owner_full_name owner_is_private owner_is_unpublished  
tiering_recommendation owner_media_count owner_edge_followed_by_count is_ad multiple_photos video_duration  
product_type  