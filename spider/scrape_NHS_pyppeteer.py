import asyncio
import datetime
import json
import logging
import pymongo
import re
from urllib.parse import urljoin

from pyquery import PyQuery as pq
from pyppeteer import launch
from pyppeteer.errors import TimeoutError

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s:%(message)s')

BASE_URL = 'http://www.nhc.gov.cn/'
TIMEOUT = 10
TOTAL_PAGE = 10
WINDOW_WIDTH, WINDOW_HEIGHT = 1366, 768
HEADLESS = True

MONGO_CONNECTION_STRING = 'mongodb://localhost:27017'
MONGO_DB_NAME = 'spider'
MONGO_COLLECTION_NAME = 'COVID-19'
client = pymongo.MongoClient(MONGO_CONNECTION_STRING, connect=False)
db = client['spider']
collection = db['COVID-19']


async def init():
    global browser, tab
    browser = await launch(headless=HEADLESS,
                           args=['--disable-infobars',
                                 f'--window-size={WINDOW_WIDTH},{WINDOW_HEIGHT}'])
    tab = await browser.newPage()
    await tab.setViewport({'width': WINDOW_HEIGHT, 'height': WINDOW_HEIGHT})
    await tab.evaluateOnNewDocument('Object.defineProperty(navigator,"webdriver",{get:()=>undefined})')
    await tab.setViewport({'width': WINDOW_WIDTH, 'height': WINDOW_HEIGHT})


async def scrape_page(url, selector):
    logging.info('scrape %s', url)
    try:
        await tab.goto(url)
        await tab.waitForSelector(selector, optios={'timeout': TIMEOUT * 1000})
    except TimeoutError:
        logging.error('error occurred while scrape %s', url, exc_info=True)


async def scrape_total_page():
    try:
        await tab.goto('http://www.nhc.gov.cn/xcs/yqtb/list_gzbd.shtml')
        await tab.waitForSelector('.pagination_index_last')
        html = await tab.content()
        doc = pq(html)
        page = doc('.pagination_index_last').text()
        return int(re.search('共\s(\d+)\s页', page).group(1))
    except:
        logging.error('error occurred while get pages', exc_info=True)


async def scrape_index(page):
    if page == 1:
        index_url = f'{BASE_URL}xcs/yqtb/list_gzbd.shtml'
    else:
        index_url = f'{BASE_URL}xcs/yqtb/list_gzbd_{page}.shtml'
    await scrape_page(index_url, '.zxxx_list li')


async def parse_index():
    # elements = await tab.querySelectorAllEval('.zxxx_list li a', 'nodes=>nodes.map(node=>node.innerText)')
    # for element in elements:
    #     if re.search('截至\d+月\d+日24时新型冠状病毒肺炎疫情最新情况', element):
            detail_urls = await tab.querySelectorAllEval('.zxxx_list li a', 'nodes=>nodes.map(node=>node.href)')
            return detail_urls


async def scrape_detail(url):
    await scrape_page(url, '#xw_box')


async def parse_detail():
    url = tab.url
    title = await tab.querySelectorEval('.list .tit', 'node=>node.innerText')
    doc = pq(await tab.content())
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


async def main():
    await init()
    try:
        total_page = await scrape_total_page()
        for page in range(1, total_page + 1):
            await scrape_index(page)
            detail_urls = await parse_index()
            for detail_url in detail_urls:
                logging.info('detail url %s', urljoin(BASE_URL, detail_url))
                await scrape_detail(detail_url)
                data = await parse_detail()
                logging.info('detail data %s', data)
                logging.info('save data to mongodb')
                save_data(data)
                logging.info('data saved successfully')
    finally:
        await browser.close()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
