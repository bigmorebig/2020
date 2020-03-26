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
    'Cookie': 'll="118318"; bid=OZyRH3X33WQ; _vwo_uuid_v2=D15D3C4EA3321CCCFF92EDAECA03025DB|0bd7792157fdfbc5bdce72f321a63dbf; douban-fav-remind=1; gr_user_id=f5675fe8-77f7-4713-9a10-bb98e7c06591; ct=y; __utmc=30149280; __utmc=81379588; viewed="3261600_1037602_1422833_1964774_1858513_1007305_4913064"; push_noty_num=0; push_doumail_num=0; __utmv=30149280.18714; dbcl2="187149828:nOLJg0x0fwc"; ck=SGwB; douban-profile-remind=1; gr_session_id_22c937bbd8ebd703f2d8e9445f7dfd03=791e3921-ed08-4100-b4c7-64c0cdc045a3; gr_cs1_791e3921-ed08-4100-b4c7-64c0cdc045a3=user_id%3A1; gr_session_id_22c937bbd8ebd703f2d8e9445f7dfd03_791e3921-ed08-4100-b4c7-64c0cdc045a3=true; __utma=30149280.1720937522.1578139265.1585191646.1585197963.11; __utmz=30149280.1585197963.11.9.utmcsr=book.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/subject/20440644/; __utmt=1; _pk_ref.100001.3ac3=%5B%22%22%2C%22%22%2C1585197972%2C%22https%3A%2F%2Fsearch.douban.com%2Fbook%2Fsubject_search%3Fsearch_text%3D%25E4%25B8%2596%25E7%2595%258C%25E7%259A%2584%25E5%2587%259B%25E5%2586%25AC%26cat%3D1001%22%5D; _pk_id.100001.3ac3=15cf6b64997133a2.1585138258.4.1585197972.1585194269.; _pk_ses.100001.3ac3=*; __utmt_douban=1; __utmb=30149280.3.10.1585197963; __utma=81379588.507128748.1585138258.1585191781.1585197972.4; __utmz=81379588.1585197972.4.3.utmcsr=search.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/book/subject_search; __utmb=81379588.1.10.1585197972'
}

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



