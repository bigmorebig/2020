import datetime
import logging
import re
from urllib.parse import urljoin

import pymongo
from pyquery import PyQuery as pq
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver import ChromeOptions
from selenium.webdriver.support import expected_conditions as EC

TIME_OUT = 10
option = ChromeOptions()
# option.add_argument('--headless')
option.add_experimental_option('excludeSwitches', ['enable-automation'])
option.add_experimental_option('useAutomationExtension', False)

browser = webdriver.Chrome(options=option)
browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                        {'source': 'Object.defineProperty(navigator,"webdriver",{get:()=>undefined})'})
wait = WebDriverWait(browser, TIME_OUT)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s:%(message)s')
BASE_URL = 'http://www.nhc.gov.cn/'

MONGO_CONNECTION_STRING = 'mongodb://localhost:27017'
MONGO_DB_NAME = 'spider'
MONGO_COLLECTION_NAME = 'COVID-19'
client = pymongo.MongoClient(MONGO_CONNECTION_STRING, connect=False)
db = client['spider']
collection = db['COVID-19']


def scrape_page(url, condition, locator):
    logging.info('scrape %s', url)
    try:
        browser.get(url)
        wait.until(condition(locator))
    except requests.exceptions.RequestException:
        logging.error('error occurred while scraping %s', url, exc_info=True)


def scrape_total_page():
    try:
        browser.get('http://www.nhc.gov.cn/xcs/yqtb/list_gzbd.shtml')
        html = browser.page_source
        doc = pq(html)
        page = doc('.pagination_index_last').text()
        return int(re.search('共\s(\d+)\s页', page).group(1))
    except:
        logging.error('error occurred while get pages', exc_info=True)


def scrape_index(page):
    if page == 1:
        index_url = f'{BASE_URL}xcs/yqtb/list_gzbd.shtml'
    else:
        index_url = f'{BASE_URL}xcs/yqtb/list_gzbd_{page}.shtml'
    scrape_page(index_url, condition=EC.visibility_of_all_elements_located, locator=(By.CSS_SELECTOR, '.zxxx_list li'))


def parse_index():
    elements = browser.find_elements_by_css_selector('.zxxx_list li a')
    for element in elements:
        if re.search('截至\d+月\d+日24时新型冠状病毒肺炎疫情最新情况', element.text):
            detail_url = element.get_attribute('href')
            yield urljoin(BASE_URL, detail_url)


def scrape_detail(url):
    scrape_page(url, condition=EC.presence_of_element_located, locator=(By.CSS_SELECTOR, '#xw_box'))


def parse_detail():
    url = browser.current_url
    title = browser.find_element_by_css_selector('.list .tit').text
    doc = pq(browser.page_source)
    source = doc('.list .source').text()
    published = re.search('\d{4}-\d{2}-\d{2}', source).group()
    source = re.search('来源:\s(.*)', source).group(1)
    content = doc('#xw_box p').text()
    updated = datetime.datetime.now()
    return {
        'url': url,
        'title': title,
        'published': published,
        'source': source,
        'content': content,
        'updated': updated
    }


def save_data(data):
    collection.update_one({
        'title': data.get('title')},
        {'$set': data},
        upsert=True
    )


def main():
    try:
        total_page = scrape_total_page()
        for page in range(1, total_page + 1):
            scrape_index(page)
            detail_urls = parse_index()
            for detail_url in list(detail_urls):
                logging.info('detail url %s', detail_url)
                scrape_detail(detail_url)
                data = parse_detail()
                logging.info('detail data %s', data)
                logging.info('save data to mongodb')
                save_data(data)
                logging.info('data saved successfully')
    finally:
        browser.close()


if __name__ == '__main__':
    main()
