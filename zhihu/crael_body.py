import requests
import json
import os
import time
import random
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import csv
from datetime import datetime


class Zhihu_BodyCrawler:
    def __init__(self):
        self.headers = self.get_headers()

    def get_headers(self, open_path='zhihu/zhihu_cookie_.json'):
        with open(open_path, 'r', encoding='utf-8') as f:
            headers = json.load(f)
        return headers

    def crawl_body_from_articles(self, url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # 获取文章标题
            title = soup.find('h1').get_text(strip=True) if soup.find('h1') else '无标题'
            # 文章id
            article_id = url.split('/')[-1] if url else '无ID'