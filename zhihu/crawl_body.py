from crawl_img import parse_page
import requests
import json
import os
import time
import random
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import csv
from datetime import datetime
import re

# 添加路径
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


class Zhihu_BodyCrawler:
    def __init__(self):
        self.headers = self.get_headers()

    def get_headers(self, open_path='zhihu/zhihu_cookie_.json'):
        with open(open_path, 'r', encoding='utf-8') as f:
            headers = json.load(f)
        return headers

    def save_to_csv(self, data, csv_file='zhihu/zhihu_data.csv', is_append=True):
        # 检查文件是否存在
        file_exists = os.path.isfile(csv_file)
        # 增量追加数据
        # 如果表格不存在，或者里面没有数据，则写入表头
        with open(csv_file, 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            if not file_exists or os.stat(csv_file).st_size == 0:
                # 写入表头
                writer.writerow(['article_id', 'question_id', 'answer_id', 'title', 'content', 'img_urls',
                                 'publish_time', 'location', 'author_id', 'author_name',
                                 'gender', 'vote_count', 'comment_count'])
            # 写入数据
            writer.writerow([
                data.get('article_id', ''),
                data.get('question_id', ''),
                data.get('answer_id', ''),
                data.get('title', ''),
                data.get('content', ''),
                data.get('img_urls', ''),
                data.get('publish_time', ''),
                data.get('location', ''),
                data.get('author_id', ''),
                data.get('author_name', ''),
                data.get('gender', -1),
                data.get('vote_count', 0),
                data.get('comment_count', 0)
            ])

        print(f"数据已保存到 {csv_file}")

    def crawl_body_from_articles(self, url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # 处理内容中的图片链接
            img_info = parse_page(response.text, url)
            img_urls = [img['link'] for img in img_info]
            full_info = soup.find('script', id='js-initialData')
            with open('author_info.txt', 'w', encoding='utf-8') as f:
                f.write(json.dumps(json.loads(full_info.string)['initialState'], ensure_ascii=False, indent=4))
            if full_info:
                try:
                    article_id = url.split('/')[-1]
                    article_info = json.loads(full_info.string)['initialState']['entities']['articles'].get(article_id, {})
                    title = article_info.get('title', '无标题')
                    # 封面图片处理
                    cover_image = article_info.get('titleImage', '')
                    if cover_image != '':
                        img_urls.append(cover_image)
                    content = article_info.get('content', '无内容')
                    publish_time = article_info.get('created', '无时间')
                    location = article_info.get('ipInfo', '无地点')
                    author_info = article_info.get('author', {})
                    author_id = author_info.get('id', '无作者ID')
                    author_name = author_info.get('name', '无作者ID')
                    gender = author_info.get('gender', -1)
                    vote_count = article_info.get('voteupCount', 0)
                    comment_count = article_info.get('commentCount', 0)
                    if publish_time:
                        publish_time = datetime.fromtimestamp(publish_time).strftime('%Y-%m-%d %H:%M:%S')
                except (KeyError, json.JSONDecodeError):
                    print("解析数据时发生错误，可能是页面结构变化或数据格式不正确")
                    title = '无标题'
                    content = '无内容'
                    publish_time = '无时间'
                    location = '无地点'
                    author_id = '无作者ID'
                    author_name = '无作者ID'

            return {
                'article_id': str(article_id),
                'question_id': '',
                'answer_id': '',
                'title': title,
                'content': content,
                'img_urls': str(img_urls),
                'publish_time': str(publish_time),
                'location': location,
                'author_id': author_id,
                'author_name': author_name,
                'gender': gender,
                'vote_count': vote_count,
                'comment_count': comment_count,
            }

        else:
            print(f"请求失败，状态码: {response.status_code}")
            return None

    def crawl_body_from_answers(self, url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            img_info = parse_page(response.text, url)
            img_urls = [img['link'] for img in img_info]
            full_info = soup.find('script', id='js-initialData')
            with open('author_info.txt', 'w', encoding='utf-8') as f:
                f.write(json.dumps(json.loads(full_info.string)['initialState'], ensure_ascii=False, indent=4))
            if full_info:
                try:
                    question = json.loads(full_info.string)['initialState']['entities']['questions'].get(url.split('/')[-3], {}).get('title', '')
                    details = json.loads(full_info.string)['initialState']['entities']['questions'].get(url.split('/')[-3], {}).get('detail', '')
                    img_in_question = json.loads(full_info.string)['initialState']['entities']['questions'].get(url.split('/')[-3], {}).get('thumbnailInfo', {}).get('thumbnails', '')
                    if img_in_question:
                        for item in img_in_question:
                            if item.get('type') == 'image':
                                img_urls.append(item['url'])
                    title = question + ' - ' + details if question else '无标题'
                    author = json.loads(full_info.string)['initialState']['entities']['answers'].get(url.split('/')[-1], {})
                    content = author.get('content', '无内容')
                    location = author.get('ipInfo', '无地点')
                    author_id = author.get('author', {}).get('id', '无作者ID')
                    gender = author.get('author', {}).get('gender', -1)
                    author_name = author.get('author', {}).get('name', '无作者ID')
                    vote_count = author.get('voteupCount', 0)
                    comment_count = author.get('commentCount', 0)
                    publish_time = author.get('createdTime', '无时间')
                    if publish_time:
                        publish_time = datetime.fromtimestamp(publish_time).strftime('%Y-%m-%d %H:%M:%S')
                except (KeyError, json.JSONDecodeError):
                    pass

            return {
                'article_id': '',
                'question_id': url.split('/')[-3] if 'question' in url else '',
                'answer_id': url.split('/')[-1],
                'title': title,
                'content': content,
                'img_urls': str(img_urls),
                'publish_time': str(publish_time),
                'location': location,
                'author_id': author_id,
                'author_name': author_name,
                'gender': gender,
                'vote_count': vote_count,
                'comment_count': comment_count
            }
        else:
            print(f"请求失败，状态码: {response.status_code}")
            return None


if __name__ == "__main__":
    crawler = Zhihu_BodyCrawler()
    # url = "https://zhuanlan.zhihu.com/p/30201040247"  # 替换为实际的知乎专栏文章URL
    # url = "https://www.zhihu.com/question/19843390/answer/1671468403"
    # url = "https://www.zhihu.com/question/448360134/answer/3499122968"
    # url = "https://www.zhihu.com/question/271551679/answer/2957931212"
    # url = "https://zhuanlan.zhihu.com/p/1892956238343565443"
    # url = "https://zhuanlan.zhihu.com/p/1911092673873420373"

    # article_data = crawler.crawl_body_from_articles(url)
    # # article_data = crawler.crawl_body_from_answers(url)
    # if article_data:
    #     # print(json.dumps(article_data, ensure_ascii=False, indent=4))  # indent=4 for pretty print
    #     # 保存到一个txt 文件看看情况
    #     with open('article.txt', 'w', encoding='utf-8') as f:
    #         f.write(json.dumps(article_data, ensure_ascii=False, indent=4))

    # else:
    #     print("未能获取文章数据")

    # 读取zhihu_urls.txt中的URL
    with open('zhihu/zhihu_urls.txt', 'r', encoding='utf-8') as f:
        urls = f.readlines()
    urls = [url.strip() for url in urls if url.strip()]  # 去除空行和多余空格
    print(f"读取到 {len(urls)} 个URL")
    for i in range(244, len(urls)):
        url = urls[i]
        print(f"正在处理第 {i + 1} 个URL: {url}")
        if 'zhuanlan.zhihu.com' in url:
            data = crawler.crawl_body_from_articles(url)
        elif 'zhihu.com/question' in url and 'answer' in url:
            data = crawler.crawl_body_from_answers(url)
        else:
            print(f"URL格式不正确: {url}")
            continue

        if data:
            crawler.save_to_csv(data, csv_file='zhihu/zhihu_data.csv', is_append=True)
            print(f"第 {i + 1} 个URL处理完成\n")
        # 爬完一个URL后，随机等待5到10秒
        wait_time = random.randint(5, 10)
        print(f"等待 {wait_time} 秒...")
        time.sleep(wait_time)
