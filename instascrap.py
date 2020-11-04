from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import sys
import mysql.connector


SCROLL_PAUSE_TIME = 2
URL = 'https://www.instagram.com/explore/tags/corona/'


def login(driver, usermail, password):
    """
    This function log you in to instagram by using your facebook account, before starting to scrap
    """
    driver.find_element_by_xpath('//*[@id="react-root"]/section/nav/div[2]/div/div/div[3]/div/span/a[1]/button').click()
    time.sleep(SCROLL_PAUSE_TIME)
    driver.find_element_by_xpath('//*[@id="loginForm"]/div/div[5]/button').click()
    time.sleep(SCROLL_PAUSE_TIME)
    driver.find_element_by_xpath('//*[@id="email"]').send_keys(usermail)
    driver.find_element_by_xpath('//*[@id="pass"]').send_keys(password)
    driver.find_element_by_xpath('//*[@id="loginbutton"]').click()
    time.sleep(SCROLL_PAUSE_TIME)


def html_line_of_posts(driver):
    """
    generator of rows in instagram page, each row has 3 or 4 posts.
    """
    start = 0
    doc = BeautifulSoup(driver.page_source, "html.parser")
    row = doc.find('article').findChildren("div", recursive=False)[1]
    lines = row.find("div", recursive=False).findChildren("div", recursive=False)
    total_lines_on_page = len(row.find("div", recursive=False).findChildren("div", recursive=False))
    while True:
        if start < total_lines_on_page:
            yield lines[start]
            start += 1
        else:
            height = driver.execute_script("return document.documentElement.scrollHeight")
            driver.execute_script("window.scrollTo(0, " + str(height) + ");")
            time.sleep(SCROLL_PAUSE_TIME)
            doc = BeautifulSoup(driver.page_source, "html.parser")
            row = doc.find('article').findChildren("div", recursive=False)[1]
            lines = row.find("div", recursive=False).findChildren("div", recursive=False)
            total_lines_on_page = len(lines)
            start = 0


def main():
    args = sys.argv[:]

    if not args:
        print('usage: instacrap.py mysqluser mysqlpass usermail_facebook password_facebook'
              'if you want to scrap without login using facebook user, use _ at usermail_facebook and password_facebook'
              'for example: instacrap.py SQLUSER SQLPASS _ _')
        sys.exit(1)


    mysqluser = args[1]
    mysqlpass = args[2]
    # for connection to instagram by using facebook account
    usermail = args[3]
    password = args[4]

    # MySQL connection to db
    mydb = mysql.connector.connect(
        host="localhost",
        user=mysqluser,
        password=mysqlpass,
        database="itc1"
    )

    mycursor = mydb.cursor()

    chrome_options = Options()
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--headless")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(URL)
    if usermail != '_' and password != '_':
        login(driver, usermail, password)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(SCROLL_PAUSE_TIME)

    to_postlink = []
    for line in html_line_of_posts(driver):
        for col in line.find("div", recursive=False):
            print(col.get('href'))
            link = col.get('href')
            to_postlink.append((link,))
            if len(to_postlink) == 10:
                sql = "INSERT IGNORE INTO postlink (link, is_downloaded) VALUES (%s,DEFAULT)"
                mycursor.executemany(sql, to_postlink)
                mydb.commit()
                to_postlink = []


if __name__ == '__main__':
    main()