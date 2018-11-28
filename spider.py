#!/usr/bin/env python 3
# encoding: utf-8
'''
@author: Timz
@file: spider.py
@time: 2018/11/12 15:19
'''
from urllib.parse import urlencode
import re
import pymongo
import requests
from bs4 import BeautifulSoup
from pandas._libs import json
from requests.exceptions import RequestException
from config import *
from hashlib import md5
import os
from multiprocessing import Pool

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]



headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36'
}
proxy_pool_url = 'http://127.0.0.1:5000/get'





def get_proxy():
    try:
        response = requests.get(proxy_pool_url)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        return None

def get_page_index(offset, KEYWORD, proxies):
    data = {
        'offset': offset,
        'format': 'json',
        'keyword': KEYWORD,
        'autoload': 'true',
        'count': 20,
        'cur_tab': 3,
        'from': 'gallery',
        'pd':' ',
    }
    url = 'https://www.toutiao.com/search_content/?' + urlencode(data)
    try:
        response = requests.get(url, headers = headers, proxies = proxies)
        if response.status_code == 200:
            print('正在解析列表' + url)
            return response.text
        return None
    except RequestException:
        print('请求索引页出错')
        return None

def parse_page_index(html):
    data = json.loads(html)
    if data and 'data' in data.keys():
        for item in data.get('data'):
            yield item.get('article_url')


def get_page_datail(url, proxies):
    try:
        response = requests.get(url, headers = headers, proxies = proxies)
        if response.status_code == 200:
            print('解析详情页'+ url)
            return response.text
        return None
    except RequestException:
        print('请求详情页出错',url)
        return None

def parse_page_detail(html,url,proxies):
    soup = BeautifulSoup(html,'lxml')
    title = soup.select('title')[0].get_text()
    print(title)
    images_pattern = re.compile('gallery: JSON.parse\("(.*?)"\)',re.S)
    result = re.search(images_pattern, html)
    if result:
        html =result.group(1)
        html = html.replace('\\\\', '\\')
        html = html.replace(r'\"', '"')
        html = html.replace(r'\/', '/')
        data = json.loads(html)
        if data and 'sub_images' in data.keys():
            sub_images = data.get('sub_images')
            images = [item.get('url') for item in sub_images]
            for image in images:
                download_image(image,proxies)
            return {
                'title': title,
                'url': url,
                'images': images
            }

def save_to_mongo(result):
    if db[MONGO_TABLE].insert(result):
        print('存储到monboDB成功',result)
        return True
    return False

def download_image(url,proxies):
    print('正在下载',url)
    try:
        response = requests.get(url, headers = headers, proxies = proxies)
        if response.status_code == 200:
            save_image(response.content)
            return response.text
        return None
    except RequestException:
        print('请求图片出错',url)
        return None

def save_image(content):
    file_path='{0}/{1}.{2}'.format(os.getcwd()+'/images',md5(content).hexdigest(), 'jpg')
    if not os.path.exists(file_path):
        with open(file_path, 'wb') as f:
            f.write(content)
            f.close()


def main(offset):
    proxy = get_proxy()
    proxies = {
        'http' : 'http://' + proxy,
    }
    print('代理IP：' + proxy )
    html = get_page_index(offset,KEYWORD,proxies)
    for url in parse_page_index(html):
        #url = url.replace('group/','a')
        html = get_page_datail(url,proxies)
        if html:
            result = parse_page_detail(html,url,proxies)
            if result:
                save_to_mongo(result)


if __name__ == '__main__':
    groups = [x*20 for x in range(GROUP_START,GROUP_END + 1)]
    pool = Pool()
    pool.map(main,groups)