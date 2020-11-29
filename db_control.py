import mysql.connector
from copy import deepcopy

DB_NAME = 'itc1'


class DBControl():
    def __init__(self, database=DB_NAME, host="localhost", user="root", password=""):
        '''
        To change password in mysql line:
        ALTER USER 'root'@'localhost' IDENTIFIED BY '';
        '''
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.create_db()
        self.create_tables()

    def create_db(self):
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
            cond += DB_NAME in i
            if cond == True:
                break

        if bool(cond) is False:
            self.cursor.execute(f"CREATE DATABASE {DB_NAME}")

        self.cursor.close()
        self.mydb.close()


        self.mydb = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.cursor = self.mydb.cursor()


    def create_tables(self):
        self.cursor.execute("SHOW TABLES")

        tables = ['post_to_scrap', 'post_info', 'post_content', 'owner', 'location']
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
                self.cursor.execute("CREATE TABLE post_to_scrap "
                                 "(shortcode VARCHAR(30) PRIMARY KEY NOT NULL, is_scraped BOOL DEFAULT 0,"
                                 "in_process BOOL DEFAULT 0,"
                                 "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")

            if table == 'post_info':
                self.cursor.execute("CREATE TABLE post_info "
                                 "(shortcode VARCHAR(30) PRIMARY KEY NOT NULL, id BIGINT, hashtags VARCHAR(500),"
                                 "owner_id BIGINT, location_id BIGINT, type VARCHAR(30),"
                                 "dim_height INT, dim_width INT, is_video BOOL, comment_count INT,"
                                 "preview_comment_count INT, comment_disabled BOOL, timestamp INT,"
                                 "like_count INT, is_ad BOOL, video_duration REAL, product_type VARCHAR(10),"
                                 "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")

            if table == 'post_content':
                """
                """
                self.cursor.execute("CREATE TABLE post_content "
                                 "(shortcode VARCHAR(30) PRIMARY KEY NOT NULL, photo_url VARCHAR(1500),"
                                 "ai_comment TEXT, post_text TEXT, comments TEXT, preview_comment TEXT,"
                                 "location_name VARCHAR(100), multiple_photos TEXT,"
                                 "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")


            if table == 'owner':
                """
                """
                self.cursor.execute("CREATE TABLE owner "
                                 "(id BIGINT PRIMARY KEY NOT NULL, is_verified BOOL,"
                                 "profile_pic_url VARCHAR(2083), username VARCHAR(100),"
                                 "full_name VARCHAR(100), is_private BOOL, is_unpublished BOOL,"
                                 "tiering_recommendation BOOL, media_count INT, followed_by_count INT,"
                                 "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")



            if table == 'location':
                """
                """
                self.cursor.execute("CREATE TABLE location "
                                 "(id BIGINT PRIMARY KEY NOT NULL, has_public_page BOOL,"
                                 "slug VARCHAR(150), json VARCHAR(1000),"
                                 "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")



    def insert_shortcodes(self, shortcodes):
        """
        shortcodes = [('/p/B_BaT-sBiPq/',), ('/p/B_HxtctgVao/',), ('/p/B_ioY0aHz3V/',)]
        shortcodes = ['/p/B_BaT-sBiPq/', '/p/B_HxtctgVao/', '/p/B_ioY0aHz3V/']
        """
        if type(shortcodes[0]) is str:
            shortcodes = list(map(lambda x: (x,), shortcodes))
        sql = "INSERT IGNORE INTO post_to_scrap (shortcode) VALUES (%s)"
        self.cursor.executemany(sql, shortcodes)
        self.mydb.commit()


    def confirm_end_scraping_for_shortcodes(self, shortcodes):
        """
        shortcodes = [('/p/B_BaT-sBiPq/',), ('/p/B_HxtctgVao/',), ('/p/B_ioY0aHz3V/',)]
        shortcodes = ['/p/B_BaT-sBiPq/', '/p/B_HxtctgVao/', '/p/B_ioY0aHz3V/']
        """
        if type(shortcodes[0]) is str:
            shortcodes = list(map(lambda x: (x,), shortcodes))

        sql = "UPDATE post_to_scrap SET is_scraped = 1, in_process = 0 WHERE shortcode=%s"
        self.cursor.executemany(sql, shortcodes)
        self.mydb.commit()


    def unconfirm_end_scraping_for_shortcodes(self, shortcodes):
        """
        shortcodes = [('/p/B_BaT-sBiPq/',), ('/p/B_HxtctgVao/',), ('/p/B_ioY0aHz3V/',)]
        shortcodes = ['/p/B_BaT-sBiPq/', '/p/B_HxtctgVao/', '/p/B_ioY0aHz3V/']
        """
        if type(shortcodes[0]) is str:
            shortcodes = list(map(lambda x: (x,), shortcodes))

        sql = "UPDATE post_to_scrap SET is_scraped = 0, in_process = 0 WHERE shortcode=%s"
        self.cursor.executemany(sql, shortcodes)
        self.mydb.commit()


    def shortcodes_list_for_scraping(self, limit=16):
        """
        shortcodes = [('/p/B_BaT-sBiPq/',), ('/p/B_HxtctgVao/',), ('/p/B_ioY0aHz3V/',)]
        shortcodes = ['/p/B_BaT-sBiPq/', '/p/B_HxtctgVao/', '/p/B_ioY0aHz3V/']
        """
        self.cursor.execute(f"SELECT shortcode FROM post_to_scrap WHERE is_scraped = 0 AND in_process = 0 LIMIT {limit}")
        shortcodes = list(self.cursor)

        sql = "UPDATE post_to_scrap SET in_process = 1 WHERE shortcode=%s"
        self.cursor.executemany(sql, shortcodes)
        self.mydb.commit()
        shortcodes = list(map(lambda x: x[0], shortcodes))
        return shortcodes


    def check_post_to_scrap_sanity(self):
        self.cursor.execute(f"SELECT shortcode FROM post_to_scrap WHERE is_scraped = 0 AND in_process = 1")
        shortcodes = list(self.cursor)

        sql = "UPDATE post_to_scrap SET in_process = 0 WHERE shortcode=%s"
        self.cursor.executemany(sql, shortcodes)
        self.mydb.commit()


    def insert_posts(self, post_array):
        self.insert_post_info(post_array)
        self.insert_post_content(post_array)
        self.insert_owner(post_array)
        self.insert_location(post_array)
        self.update_post_to_scrap(post_array)


    def update_post_to_scrap(self, post_array):
        shortcodes_scraped = []
        for post in post_array:
            shortcodes_scraped.append(post['shortcode'][0])

        self.cursor.execute(f"SELECT shortcode FROM post_content WHERE shortcode IN {tuple(shortcodes_scraped)}")
        updated_shortcode_checked = list(map(lambda x: x[0], list(self.cursor)))
        if len(updated_shortcode_checked) > 0:
            self.confirm_end_scraping_for_shortcodes(updated_shortcode_checked)

        shortcodes_to_scrap_not_validated = list(set(shortcodes_scraped) - set(updated_shortcode_checked))
        if len(shortcodes_to_scrap_not_validated) > 0:
            self.confirm_end_scraping_for_shortcodes(shortcodes_to_scrap_not_validated)


    def insert_location(self, post_array):
        location_insert = []
        for post in post_array:
            try:
                location_id = post['location_id'][0]
            except KeyError:
                continue
            try:
                has_public_page = int(post['location_has_public_page'][0])
            except KeyError:
                has_public_page = None
            try:
                slug = post['location_slug'][0]
            except KeyError:
                slug = None
            try:
                json_field = post['location_json'][0]
            except KeyError:
                json_field = None

            print(location_id, has_public_page, slug, json_field)
            print(type(location_id), type(has_public_page), type(slug), type(json_field))

            post_columns_location = (location_id, has_public_page, slug, json_field)
            location_insert.append(deepcopy(post_columns_location))

        sql = f"INSERT INTO location (id, has_public_page, slug, json) " \
              f"VALUES (%s ,%s, %s ,%s) ON DUPLICATE KEY UPDATE has_public_page=VALUES(has_public_page), slug=VALUES(slug), json=VALUES(json)"
        self.cursor.executemany(sql, location_insert)
        self.mydb.commit()


    def insert_owner(self, post_array):
        owner_insert = []
        for post in post_array:
            owner_id = post['owner_id'][0]
            is_verified = int(post['owner_is_verified'][0])
            profile_pic_url = post['owner_profile_pic_url'][0]
            username = post['owner_username'][0]
            full_name = post['owner_full_name'][0]
            is_private = int(post['owner_is_private'][0])
            is_unpublished = int(post['owner_is_unpublished'][0])
            tiering_recommendation = int(post['tiering_recommendation'][0])
            media_count = int(post['owner_media_count'][0])
            followed_by_count = int(post['owner_edge_followed_by_count'][0])


            print(owner_id, is_verified, profile_pic_url, username, full_name, is_private, is_unpublished, tiering_recommendation, media_count, followed_by_count)
            print(type(owner_id), type(is_verified), type(profile_pic_url), type(username), type(full_name), type(is_private), type(is_unpublished),
                  type(tiering_recommendation), type(media_count), type(followed_by_count))

            post_columns_owner = (owner_id, is_verified, profile_pic_url, username, full_name, is_private, is_unpublished, tiering_recommendation, media_count, followed_by_count)
            owner_insert.append(deepcopy(post_columns_owner))

        sql = f"INSERT INTO owner (id, is_verified, profile_pic_url, username, full_name, is_private, is_unpublished, tiering_recommendation, media_count, followed_by_count) " \
              f"VALUES (%s ,%s, %s ,%s, %s ,%s, %s ,%s, %s ,%s) ON DUPLICATE KEY UPDATE is_verified=VALUES(is_verified), profile_pic_url=VALUES(profile_pic_url), " \
              f"username=VALUES(username), full_name=VALUES(full_name), is_private=VALUES(is_private), is_unpublished=VALUES(is_unpublished), " \
              f"tiering_recommendation=VALUES(tiering_recommendation), media_count=VALUES(media_count), followed_by_count=VALUES(followed_by_count)"
        self.cursor.executemany(sql, owner_insert)
        self.mydb.commit()


    def insert_post_content(self, post_array):
        content_insert = []
        for post in post_array:
            shortcode = post['shortcode'][0]
            try:
                photo_url = str(post['photo_url'][0])
            except KeyError:
                photo_url = None
            try:
                ai_comment = post['ai_comment'][0]
            except KeyError:
                ai_comment = None
            try:
                post_text = str(post['post_text'][0])
            except KeyError:
                post_text = None
            try:
                comments = str(post['comments'][0])
            except KeyError:
                comments = None
            try:
                preview_comment = str(post['preview_comment'][0])
            except KeyError:
                preview_comment = None
            try:
                location_name = post['location_name'][0]
            except KeyError:
                location_name = None
            try:
                multiple_photos = str(post['multiple_photos'][0])
            except KeyError:
                multiple_photos = None

            print(shortcode, photo_url, ai_comment, post_text, comments, preview_comment, location_name, multiple_photos)
            print(type(shortcode), type(photo_url), type(ai_comment), type(post_text), type(comments), type(preview_comment), type(location_name),
                  type(multiple_photos))

            post_columns_content = (shortcode, photo_url, ai_comment, post_text, comments, preview_comment, location_name, multiple_photos)
            content_insert.append(deepcopy(post_columns_content))

        sql = f"INSERT INTO post_content (shortcode, photo_url, ai_comment, post_text, comments, preview_comment, location_name, multiple_photos) " \
              f"VALUES (%s ,%s, %s ,%s, %s ,%s, %s ,%s) ON DUPLICATE KEY UPDATE comments=VALUES(comments), multiple_photos=VALUES(multiple_photos), " \
              f"preview_comment=VALUES(preview_comment), ai_comment=VALUES(ai_comment)"
        self.cursor.executemany(sql, content_insert)
        self.mydb.commit()



    def insert_post_info(self, post_array):
        posts_insert = []
        for post in post_array:
            shortcode = post['shortcode'][0]
            post_id = post['id'][0]
            owner_id = post['owner_id'][0]
            try:
                location_id = post['location_id'][0]
            except KeyError:
                location_id = None
            try:
                hashtags = ", ".join(post['hashtags'][0])
            except KeyError:
                hashtags = None
            post_type = post['type'][0]
            dim_height = int(post['dim_height'][0])
            dim_width = int(post['dim_width'][0])
            is_video = int(post['is_video'][0])
            comment_count = int(post['comment_count'][0])
            try:
                preview_comment_count = int(post['preview_comment_count'][0])
            except KeyError:
                preview_comment_count = None

            try:
                comment_disabled = int(post['comments_disabled'][0])
            except KeyError:
                comment_disabled = None

            try:
                timestamp = int(post['timestamp'][0])
            except KeyError:
                timestamp = None

            try:
                like_count = int(post['like_count'][0])
            except KeyError:
                like_count = None

            try:
                is_ad = int(post['is_ad'][0])
            except KeyError:
                is_ad = None

            try:
                video_duration = float(post['video_duration'][0])
            except KeyError:
                video_duration = None

            try:
                product_type = post['product_type'][0]
            except KeyError:
                product_type = None
            print(shortcode, post_id, hashtags, owner_id, location_id, post_type, dim_height, dim_width, is_video, comment_count,
                  preview_comment_count, comment_disabled, timestamp, like_count, is_ad, video_duration, product_type)

            post_columns_content = (shortcode, post_id, hashtags, owner_id, location_id, post_type, dim_height, dim_width, is_video,
                                           comment_count, preview_comment_count, comment_disabled, timestamp, like_count, is_ad, video_duration, product_type)
            posts_insert.append(deepcopy(post_columns_content))

        sql = f"INSERT INTO post_info (shortcode, id, hashtags, owner_id, location_id, type, dim_height, dim_width, is_video, comment_count, " \
                  f"preview_comment_count, comment_disabled, timestamp, like_count, is_ad, video_duration, product_type) " \
              f"VALUES (%s ,%s, %s, %s, %s, %s ,%s, %s ,%s, %s ,%s ,%s ,%s ,%s ,%s ,%s ,%s) ON DUPLICATE KEY UPDATE comment_count=VALUES(comment_count), " \
              f"preview_comment_count=VALUES(preview_comment_count), comment_disabled=VALUES(comment_disabled)"
        self.cursor.executemany(sql, posts_insert)
        self.mydb.commit()


def main():
    shortcodes = [('/p/B_BaT-sBiPq/',), ('/p/B_HxtctgVao/',), ('/p/B_ioY0aHz3V/',)]
    # shortcodes = ['/p/B_BaT-sBiPq/', '/p/B_HxtctgVao/', '/p/B_ioY0aHz3V/']

    dbc = DBControl()
    dbc.insert_shortcodes(shortcodes)
    # dbc.confirm_end_scraping_for_shortcodes(shortcodes[1:3])
    shortcodes1 = dbc.shortcodes_list_for_scraping(limit=1)
    print(shortcodes1)

if __name__ == '__main__':
    main()