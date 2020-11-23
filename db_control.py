import mysql.connector

DB_NAME = 'itc1'

class DBControl():

    def __init__(self, database=DB_NAME, host="localhost", user="yairstemmer", password="1234"):
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
                                 "(shortcode VARCHAR(30) PRIMARY KEY NOT NULL, id INT,"
                                 "owner_id INT, location_id INT, type VARCHAR(10),"
                                 "dim_height INT, dim_width INT, is_video BOOL, comment_count INT,"
                                 "preview_comment_count INT, comment_disabled BOOL, timestamp INT,"
                                 "like_count INT, is_ad BOOL, video_duration INT, product_type VARCHAR(10),"
                                 "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")

            if table == 'post_content':
                """
                url length is unknown, 2083 is the maximum size for a varchar, it is still more efficient than TEXT type
                """
                self.cursor.execute("CREATE TABLE post_content "
                                 "(shortcode VARCHAR(30) PRIMARY KEY NOT NULL, photo_url VARCHAR(2083),"
                                 "ai_comment TEXT, post_text TEXT, comments TEXT, preview_comment TEXT,"
                                 "location_name VARCHAR(100), multiple_photos TEXT,"
                                 "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")


            if table == 'owner':
                """
                """
                self.cursor.execute("CREATE TABLE owner "
                                 "(id INT PRIMARY KEY NOT NULL, is_verified BOOL,"
                                 "profile_pic_url VARCHAR(2083), username VARCHAR(100),"
                                 "full_name VARCHAR(100), is_private BOOL, is_unpublished BOOL,"
                                 "tiering_recommendation BOOL, media_count INT, followed_by_count INT,"
                                 "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")



            if table == 'location':
                """
                """
                self.cursor.execute("CREATE TABLE location "
                                 "(id INT PRIMARY KEY NOT NULL, has_public_page BOOL,"
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