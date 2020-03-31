from datetime import datetime
import multiprocessing

import requests
import re
import logging
import pymongo
from pyquery import PyQuery as pq

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s:%(message)s')
BASE_URL = 'https://book.douban.com/top250'
TOTAL_PAGES = 10
MONGO_CONNECTION_STRING = 'mongodb://localhost:27017'
MONGO_DB_NAME = 'spider'
MONGO_COLLECTION_NAME = 'douban'
HEADER = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
    'Cookie': '${cookies}'}

client = pymongo.MongoClient(MONGO_CONNECTION_STRING, connect=False)
db = client['spider']
collection = db['douban']


def scrap_page(url):
    logging.info('scrap %s', url)
    try:
        response = requests.get(url, headers=HEADER)
        if response.status_code == requests.codes.ok:
            return response.text
        logging.error('get invalid status code %s while scraping %s', response.status_code, url)
    except requests.RequestException:
        logging.error('error occured while scraping %s', url, exc_info=True)


def scrap_index(page):
    index_url = f'{BASE_URL}?start={page*25}'
    return scrap_page(index_url)


def parse_index(html):
    doc = pq(html)
    links = doc('.item .pl2 a')
    for link in links.items():
        detail_url = link.attr('href')
        logging.info('get detail url %s', detail_url)
        yield detail_url


def scrap_detail(url):
    return scrap_page(url)


def parse_detail(html):
    doc = pq(html)
    image = doc('.nbg img').attr('src')
    name = doc('h1 span').text()
    info = doc('#info').text()
    try:
        authors = doc('#info span a')
        authors = re.split('\[.*?]|著|续|\s|/', authors.group(1))
        author = [author for author in authors if author]
    except AttributeError:
        author = None
    published = re.search('.*?出版社:(.*?)\n', info, re.S).group(1)
    published_at = re.search('.*?出版年:(.*?)\n', info, re.S).group(1)
    try:
        pages = re.search('.*?页数:(.*?)\n', info, re.S).group(1)
        pages = [page for page in pages.split('；')]
    except ValueError:
        pages = re.search('.*?页数:(.*?)\n', info, re.S).group(1)
    except AttributeError:
        pages = None
    try:
        sales_price = float(re.search('.*?定价:.*?([\d.]+)', info, re.S).group(1))
    except AttributeError:
        sales_price = None
    ISBN = re.search('.*?(?:ISBN|统一书号):(.*)', info, re.S).group(1)
    update = datetime.now()
    return {
        'image': image,
        'name': name,
        'author': author,
        'published': published,
        'published_at': published_at,
        'pages': pages,
        'sales_price': sales_price,
        'ISBN': ISBN,
        'update': update
    }


def save_data(data):
    collection.update_one(
        {'name': data.get('name')},
        {'$set': data},
        upsert=True
    )


def main(page):
    index_html = scrap_index(page)
    detail_urls = parse_index(index_html)
    for detail_url in detail_urls:
        detail_html = scrap_detail(detail_url)
        data = parse_detail(detail_html)
        logging.info('get detail data %s', data)
        logging.info('save data to mongodb')
        save_data(data)
        logging.info('data saved successfully')


if __name__ == '__main__':
    pools = multiprocessing.Pool()
    pages = range(0, TOTAL_PAGES)
    pools.map(main, pages)
    pools.close()
    pools.join()



