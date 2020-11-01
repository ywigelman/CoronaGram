from selenium import common
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

SCROLL_PAUSE_TIME = 0.5
STOP_RETRY = 3
SCROLL_PAUSE_TIME = 2
URL = 'https://www.instagram.com/explore/tags/corona/'


chrome_options = Options()
# chrome_options.add_argument("--disable-extensions")
# chrome_options.add_argument("--disable-gpu")
# chrome_options.add_argument("--headless")

driver = webdriver.Chrome(options=chrome_options)
driver.get(URL)

Pagelength = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

time.sleep(SCROLL_PAUSE_TIME)

doc = BeautifulSoup(driver.page_source, "html.parser")
row = doc.find('article').findChildren("div", recursive=False)[1]


def html_line_of_posts():
    start = 0
    retry = 0
    doc = BeautifulSoup(driver.page_source, "html.parser")
    row = doc.find('article').findChildren("div", recursive=False)[1]
    lines = row.find("div", recursive=False).findChildren("div", recursive=False)
    total_lines_on_page = len(row.find("div", recursive=False).findChildren("div", recursive=False))
    while True:
        if start < total_lines_on_page:
            yield lines[start]
            start += 1
            retry = 0
        else:
            # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            height = driver.execute_script("return document.documentElement.scrollHeight")
            driver.execute_script("window.scrollTo(0, " + str(height) + ");")
            time.sleep(SCROLL_PAUSE_TIME)
            doc = BeautifulSoup(driver.page_source, "html.parser")
            row = doc.find('article').findChildren("div", recursive=False)[1]
            lines = row.find("div", recursive=False).findChildren("div", recursive=False)
            total_lines_on_page = len(row.find("div", recursive=False).findChildren("div", recursive=False))
            print(total_lines_on_page)
            if retry < STOP_RETRY:
                retry +=1
                continue
            else:
                print("No new value on page")
                raise StopIteration


for line in html_line_of_posts():
    for col in line.find("div", recursive=False):
        print(col.get('href'))
        #content = col.find('div', {'class':'KL4Bh'})
        content = col.find('img')
        #print(content.attrs['alt'])
        print('\n')

# nchildren = children.find("div", recursive=False)
# print(len(nchildren))
# print(nchildren)