import mysql.connector
from copy import deepcopy
import logging
from conf import *

logging.basicConfig(filename=DEFAULT_LOG_FILE_PATH, format=DEFAULT_LOG_FILE_FORMAT, level=logging.INFO)

class DBControl():
    def __init__(self, database=DB_NAME, host=DB_HOST_NAME, user=DB_USER_NAME, password=""):
        '''
        Init DBControl class with DB parameters, cursor, create DB, tables and columns
        '''
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.create_db()
        self.create_tables()

    def create_db(self):
        """
        Check if DB exist, create DB if it does not exist, and setup cursor on it
        """
        self.mydb = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
        )
        self.cursor = self.mydb.cursor()
        self.cursor.execute("SHOW DATABASES")
        existent_dbs = list(self.cursor)
        cond = False
        for i in existent_dbs:
            cond += self.database in i
            if cond == True: break

        if bool(cond) is False:
            self.cursor.execute(f"CREATE DATABASE {self.database}")

        self.cursor.close()
        self.mydb.close()

        self.mydb = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.cursor = self.mydb.cursor()
        logging.info('DB created')


    def create_tables(self):
        """
        Create all tables needed for scrapping instagram. Details on tables relation in README file
        """
        self.cursor.execute("SHOW TABLES")

        tables = ['post_to_scrap', 'owner', 'location', 'post_content', 'post_info']
        tables_to_create = []
        existent_tables = list(self.cursor)
        existent_tables = list(map(lambda x: x[0], existent_tables))

        if len(existent_tables) > 0:
            for table in tables:
                if table not in existent_tables:
                    tables_to_create.append(table)
        else:
            tables_to_create = tables

        for table in tables_to_create:
            if table == 'post_to_scrap':
                self.create_post_to_scrap_table()
            if table == 'owner':
                self.create_owner_table()
            if table == 'location':
                self.create_location_table()
            if table == 'post_info':
                self.create_post_info_table()
            if table == 'post_content':
                self.create_post_content_table()

    def create_post_to_scrap_table(self):
        """
        Create table post_to_scrap
        """
        self.cursor.execute("CREATE TABLE post_to_scrap "
                            "(shortcode VARCHAR(30) PRIMARY KEY NOT NULL, is_scraped BOOL DEFAULT 0,"
                            "in_process BOOL DEFAULT 0,"
                            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
        logging.info('Table post_to_scrap created in DB')

    def create_owner_table(self):
        """
        Create table owner
        """
        self.cursor.execute("CREATE TABLE owner "
                            "(id BIGINT PRIMARY KEY NOT NULL, is_verified BOOL,"
                            "profile_pic_url VARCHAR(2083), username VARCHAR(100),"
                            "full_name VARCHAR(100), is_private BOOL, is_unpublished BOOL,"
                            "tiering_recommendation BOOL, media_count INT, followed_by_count INT,"
                            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
        logging.info('Table owner created in DB')

    def create_location_table(self):
        """
        Create table location
        """
        self.cursor.execute("CREATE TABLE location "
                            "(id BIGINT PRIMARY KEY NOT NULL, has_public_page BOOL,"
                            "slug VARCHAR(150), json VARCHAR(1000),"
                            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
        logging.info('Table location created in DB')

    def create_post_info_table(self):
        """
        Create table post_info
        """
        self.cursor.execute("CREATE TABLE post_info "
                            "(shortcode VARCHAR(30) PRIMARY KEY NOT NULL, id BIGINT, hashtags VARCHAR(500),"
                            "owner_id BIGINT, location_id BIGINT, type VARCHAR(30),"
                            "dim_height INT, dim_width INT, is_video BOOL, comment_count INT,"
                            "preview_comment_count INT, comment_disabled BOOL, timestamp INT,"
                            "language VARCHAR(30), sentiment VARCHAR(30),"
                            "like_count INT, is_ad BOOL, video_duration REAL, product_type VARCHAR(10),"
                            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                            "CONSTRAINT FK_Shortcode1 FOREIGN KEY (shortcode) REFERENCES post_to_scrap(shortcode),"
                            "CONSTRAINT FK_owner FOREIGN KEY (owner_id) REFERENCES owner(id),"
                            "CONSTRAINT FK_location FOREIGN KEY (location_id) REFERENCES location(id),"
                            "CONSTRAINT FK_Shortcode2 FOREIGN KEY (shortcode) REFERENCES post_content(shortcode))")
        logging.info('Table post_info created in DB')

    def create_post_content_table(self):
        """
        Create table post_content
        """
        self.cursor.execute("CREATE TABLE post_content "
                            "(shortcode VARCHAR(30) PRIMARY KEY NOT NULL, photo_url VARCHAR(1500),"
                            "ai_comment TEXT, post_text TEXT, post_translation_text TEXT, comments TEXT,"
                            "preview_comment TEXT, location_name VARCHAR(100), multiple_photos TEXT,"
                            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
        logging.info('Table post_content created in DB')


    def insert_shortcodes(self, shortcodes):
        """
        insert in table post_to_scrap shortcodes that are scrapped in hashtag page
        """
        if len(shortcodes) == 0: return
        if type(shortcodes[0]) is str:
            shortcodes = list(map(lambda x: (x,), shortcodes))
        sql = "INSERT IGNORE INTO post_to_scrap (shortcode) VALUES (%s)"
        self.cursor.executemany(sql, shortcodes)
        self.mydb.commit()
        logging.debug(f'Shortcodes inserted in post_to_scrap: {", ".join(list(map(lambda x: x[0], shortcodes)))}')



    def confirm_end_scraping_for_shortcodes(self, shortcodes):
        """
        after scrapping, this function updates post_to_scrap table with status scraped
        """
        if type(shortcodes[0]) is str:
            shortcodes = list(map(lambda x: (x,), shortcodes))

        sql = "UPDATE post_to_scrap SET is_scraped = 1, in_process = 0 WHERE shortcode=%s"
        self.cursor.executemany(sql, shortcodes)
        self.mydb.commit()
        logging.debug(f'Shortcodes updated in DB in post_to_scrap: {", ".join(list(map(lambda x: x[0], shortcodes)))}')


    def unconfirm_end_scraping_for_shortcodes(self, shortcodes):
        """
        after scrapping, this function updates post_to_scrap table with status not scraped for shortcodes that were not scraped
        """
        if type(shortcodes[0]) is str:
            shortcodes = list(map(lambda x: (x,), shortcodes))

        sql = "UPDATE post_to_scrap SET is_scraped = 0, in_process = 0 WHERE shortcode=%s"
        self.cursor.executemany(sql, shortcodes)
        self.mydb.commit()
        logging.debug(f'Shortcodes not inserted in DB updated in post_to_scrap: {", ".join(list(map(lambda x: x[0], shortcodes)))}')


    def shortcodes_list_for_scraping(self, limit=16):
        """
        This function checks which shortcodes are not yet scraped
        return a limit of 16 shortcodes by default in a list
        """
        self.cursor.execute(f"SELECT shortcode FROM post_to_scrap WHERE is_scraped = 0 AND in_process = 0 LIMIT {limit}")
        shortcodes = list(self.cursor)

        sql = "UPDATE post_to_scrap SET in_process = 1 WHERE shortcode=%s"
        self.cursor.executemany(sql, shortcodes)
        self.mydb.commit()
        shortcodes = list(map(lambda x: x[0], shortcodes))
        logging.debug(f'Shortcodes list in post_to_scrap for scraping: {", ".join(shortcodes)}')
        return shortcodes


    def check_post_to_scrap_sanity(self):
        """
        after scrapping process, check in post_to_scrap if there are shortcodes that did not finish the scraping process,
        and redefine there status to not in process, in order to be recalled in the next scrapping process.
        """
        self.cursor.execute(f"SELECT shortcode FROM post_to_scrap WHERE is_scraped = 0 AND in_process = 1")
        shortcodes = list(self.cursor)

        sql = "UPDATE post_to_scrap SET in_process = 0 WHERE shortcode=%s"
        self.cursor.executemany(sql, shortcodes)
        self.mydb.commit()
        logging.debug(f'Shortcodes list sanity updated in post_to_scrap: {", ".join(list(map(lambda x: x[0], shortcodes)))}')


    def select_post_to_translate(self, number=1):
        """
        check on post_info for shortcodes that were not translated yet, in language column
        """
        self.cursor.execute(f"SELECT shortcode FROM post_info WHERE language is NULL LIMIT {number}")
        shortcodes = list(self.cursor)
        return list(map(lambda x: x[0], shortcodes))


    def select_post_text_to_translate(self, shortcode):
        """
        select post text by shortcode for translation
        """
        self.cursor.execute(f"SELECT post_text FROM post_content WHERE shortcode = '{shortcode}'")
        text = list(self.cursor)
        return text


    def update_translation_and_sentiment(self, shortcode, language, translation, sentiment):
        """
        update in db translation, language and sentiment of the post with specific shortcode
        """
        self.update_language_and_sentiment(shortcode, language, sentiment)
        if translation != None:
            self.insert_translation_to_post(shortcode, translation)
        logging.debug(f'Insert to shortcode {shortcode} translation: {translation}, '
                      f'language: {language}, sentiment: {sentiment}')


    def update_language_and_sentiment(self, shortcode, language, sentiment):
        """
        update language and sentiment of specific shortcode in post_info table
        """
        sql = "UPDATE post_info SET language=%s, sentiment=%s WHERE shortcode=%s"
        self.cursor.execute(sql, (language, sentiment, shortcode))
        self.mydb.commit()


    def insert_translation_to_post(self, shortcode, translation):
        """
        update translation in post_content table of specific shortcode
        """
        sql = "UPDATE post_content SET post_translation_text=%s WHERE shortcode=%s"
        self.cursor.execute(sql, (translation, shortcode))
        self.mydb.commit()


    def insert_posts(self, post_array):
        """
        this function distributes in all different tables of the DB the scrapping content
        """
        self.insert_post_content(post_array)
        self.insert_owner(post_array)
        self.insert_location(post_array)
        self.insert_post_info(post_array)
        self.update_post_to_scrap(post_array)


    def update_post_to_scrap(self, post_array):
        """
        this function update post_content fields to permit rescrap later for post that didn't finish scrapping
        post_array is an array of pandas table of each shortcodes scrapped
        """
        shortcodes_scraped = []
        for post in post_array:
            shortcodes_scraped.append(post['shortcode'][0])

        self.cursor.execute(f"SELECT shortcode FROM post_content WHERE shortcode IN {tuple(shortcodes_scraped)}")
        updated_shortcode_checked = list(map(lambda x: x[0], list(self.cursor)))
        if len(updated_shortcode_checked) > 0:
            self.confirm_end_scraping_for_shortcodes(updated_shortcode_checked)
            logging.debug(f'Shortcodes scrapped updated in post_to_scrap: {", ".join(updated_shortcode_checked)}')

        shortcodes_to_scrap_not_validated = list(set(shortcodes_scraped) - set(updated_shortcode_checked))
        if len(shortcodes_to_scrap_not_validated) > 0:
            self.confirm_end_scraping_for_shortcodes(shortcodes_to_scrap_not_validated)
            logging.debug(f'Shortcodes not scrapped updated in post_to_scrap: {", ".join(shortcodes_to_scrap_not_validated)}')


    def insert_location(self, post_array):
        """
        insert all posts from post_array in location table
        """
        location_insert = []
        for post in post_array:
            location_id = self.return_post_content_from_json(post, 'location_id')
            if location_id == None: continue
            has_public_page = self.return_int_post_content_from_json(post, 'location_has_public_page')
            slug = self.return_post_content_from_json(post, 'location_slug')
            json_field = self.return_post_content_from_json(post, 'location_json')

            post_columns_location = (location_id, has_public_page, slug, json_field)
            location_insert.append(deepcopy(post_columns_location))
            logging.debug(f'Location data to insert in location table: {", ".join(list(map(lambda x: str(x), post_columns_location)))}')
            logging.debug(f'Location data to insert in location table type: {", ".join(list(map(lambda x: str(type(x)), list(post_columns_location))))}')

        sql = f"INSERT INTO location (id, has_public_page, slug, json) " \
              f"VALUES (%s ,%s, %s ,%s) ON DUPLICATE KEY UPDATE has_public_page=VALUES(has_public_page), slug=VALUES(slug), json=VALUES(json)"
        self.cursor.executemany(sql, location_insert)
        self.mydb.commit()


    def insert_owner(self, post_array):
        """
        insert all posts from post_array in owner table
        """
        owner_insert = []
        for post in post_array:
            owner_id = self.return_post_content_from_json(post, 'owner_id')
            is_verified = self.return_int_post_content_from_json(post, 'owner_is_verified')
            profile_pic_url = self.return_post_content_from_json(post, 'owner_profile_pic_url')
            username = self.return_post_content_from_json(post, 'owner_username')
            full_name = self.return_post_content_from_json(post, 'owner_full_name')
            is_private = self.return_int_post_content_from_json(post, 'owner_is_private')
            is_unpublished = self.return_int_post_content_from_json(post, 'owner_is_unpublished')
            tiering_recommendation = self.return_int_post_content_from_json(post, 'tiering_recommendation')
            media_count = self.return_int_post_content_from_json(post, 'owner_media_count')
            followed_by_count = self.return_int_post_content_from_json(post, 'owner_edge_followed_by_count')

            post_columns_owner = (owner_id, is_verified, profile_pic_url, username, full_name, is_private, is_unpublished, tiering_recommendation, media_count, followed_by_count)
            owner_insert.append(deepcopy(post_columns_owner))

            logging.debug(f'Owner data to insert in owner table: {", ".join(list(map(lambda x: str(x), post_columns_owner)))}')
            logging.debug(f'Owner data to insert in owner table type: {", ".join(list(map(lambda x: str(type(x)), list(post_columns_owner))))}')

        sql = f"INSERT INTO owner (id, is_verified, profile_pic_url, username, full_name, is_private, is_unpublished, tiering_recommendation, media_count, followed_by_count) " \
              f"VALUES (%s ,%s, %s ,%s, %s ,%s, %s ,%s, %s ,%s) ON DUPLICATE KEY UPDATE is_verified=VALUES(is_verified), profile_pic_url=VALUES(profile_pic_url), " \
              f"username=VALUES(username), full_name=VALUES(full_name), is_private=VALUES(is_private), is_unpublished=VALUES(is_unpublished), " \
              f"tiering_recommendation=VALUES(tiering_recommendation), media_count=VALUES(media_count), followed_by_count=VALUES(followed_by_count)"
        self.cursor.executemany(sql, owner_insert)
        self.mydb.commit()

    def insert_post_content(self, post_array):
        """
        insert all posts from post_array in content table
        """
        content_insert = []
        for post in post_array:
            shortcode = self.return_post_content_from_json(post, 'shortcode')
            photo_url = self.return_str_post_content_from_json(post, 'photo_url')
            ai_comment = self.return_post_content_from_json(post, 'ai_comment')
            post_text = self.return_str_post_content_from_json(post, 'post_text')
            comments = self.return_str_post_content_from_json(post, 'comments')
            preview_comment = self.return_str_post_content_from_json(post, 'preview_comment')
            location_name = self.return_post_content_from_json(post, 'location_name')
            multiple_photos = self.return_str_post_content_from_json(post, 'multiple_photos')

            post_columns_content = (shortcode, photo_url, ai_comment, post_text, comments, preview_comment, location_name, multiple_photos)
            content_insert.append(deepcopy(post_columns_content))

            logging.debug(f'Post content data to insert in post_content table: {", ".join(list(map(lambda x: str(x), post_columns_content)))}')
            logging.debug(f'Post content data to insert in post_content table type: {", ".join(list(map(lambda x: str(type(x)), list(post_columns_content))))}')

        sql = f"INSERT INTO post_content (shortcode, photo_url, ai_comment, post_text, comments, preview_comment, location_name, multiple_photos) " \
              f"VALUES (%s ,%s, %s ,%s, %s ,%s, %s ,%s) ON DUPLICATE KEY UPDATE comments=VALUES(comments), multiple_photos=VALUES(multiple_photos), " \
              f"preview_comment=VALUES(preview_comment), ai_comment=VALUES(ai_comment)"
        self.cursor.executemany(sql, content_insert)
        self.mydb.commit()


    def insert_post_info(self, post_array):
        """
        insert all posts from post_array in info table
        """
        posts_insert = []
        for post in post_array:
            shortcode = self.return_post_content_from_json(post, 'shortcode')
            post_id = self.return_post_content_from_json(post, 'id')
            owner_id = self.return_post_content_from_json(post, 'owner_id')
            location_id = self.return_post_content_from_json(post, 'location_id')
            try:
                hashtags = ", ".join(post['hashtags'][0])
            except KeyError:
                hashtags = None
            post_type = self.return_post_content_from_json(post, 'type')
            dim_height = self.return_int_post_content_from_json(post, 'dim_height')
            dim_width = self.return_int_post_content_from_json(post, 'dim_width')
            is_video = self.return_int_post_content_from_json(post, 'is_video')
            comment_count = self.return_int_post_content_from_json(post, 'comment_count')
            preview_comment_count = self.return_int_post_content_from_json(post, 'preview_comment_count')
            comment_disabled = self.return_int_post_content_from_json(post, 'comments_disabled')
            timestamp = self.return_int_post_content_from_json(post, 'timestamp')
            like_count = self.return_int_post_content_from_json(post, 'like_count')
            is_ad = self.return_int_post_content_from_json(post, 'is_ad')
            video_duration = self.return_float_post_content_from_json(post, 'video_duration')
            product_type = self.return_post_content_from_json(post, 'product_type')

            post_columns_content = (shortcode, post_id, hashtags, owner_id, location_id, post_type, dim_height, dim_width, is_video,
                                           comment_count, preview_comment_count, comment_disabled, timestamp, like_count, is_ad, video_duration, product_type)
            posts_insert.append(deepcopy(post_columns_content))

            logging.debug(f'Post info data to insert in post_info table: {", ".join(list(map(lambda x: str(x), post_columns_content)))}')
            logging.debug(f'Post info data to insert in post_info table type: {", ".join(list(map(lambda x: str(type(x)), list(post_columns_content))))}')

        sql = f"INSERT INTO post_info (shortcode, id, hashtags, owner_id, location_id, type, dim_height, dim_width, is_video, comment_count, " \
                  f"preview_comment_count, comment_disabled, timestamp, like_count, is_ad, video_duration, product_type) " \
              f"VALUES (%s ,%s, %s, %s, %s, %s ,%s, %s ,%s, %s ,%s ,%s ,%s ,%s ,%s ,%s ,%s) ON DUPLICATE KEY UPDATE comment_count=VALUES(comment_count), " \
              f"preview_comment_count=VALUES(preview_comment_count), comment_disabled=VALUES(comment_disabled)"
        self.cursor.executemany(sql, posts_insert)
        self.mydb.commit()


    def return_post_content_from_json(self, dic, string):
        """
        Receive json, return the 'string' value from json, None if doesn't exist
        """
        try:
            value = dic[string][0]
        except KeyError:
            value = None
        return value

    def return_str_post_content_from_json(self, dic, string):
        """
        Receive json, return the 'string' value from json in string format, None if doesn't exist
        """
        try:
            value = str(dic[string][0])
        except KeyError:
            value = None
        return value

    def return_int_post_content_from_json(self, dic, string):
        """
        Receive json, return the 'string' value from json in int format, None if doesn't exist
        """
        try:
            value = int(dic[string][0])
        except KeyError:
            value = None
        return value

    def return_float_post_content_from_json(self, dic, string):
        """
        Receive json, return the 'string' value from json in float format, None if doesn't exist
        """
        try:
            value = float(dic[string][0])
        except KeyError:
            value = None
        return value


def test_insert_shortcode():
    """
    test shortcodes insertion in test_db
    """
    shortcodes = [('test1',), ('test2',), ('test3',)]
    dbc = DBControl(database='test_db')
    dbc.insert_shortcodes(shortcodes)
    shortcodes1 = dbc.shortcodes_list_for_scraping(limit=1)
    print(shortcodes1)
