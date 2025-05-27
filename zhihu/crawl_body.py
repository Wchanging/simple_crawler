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

    def crawl_body_from_articles(self, url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # 获取文章标题
            title = soup.find('h1').get_text(strip=True) if soup.find('h1') else '无标题'
            # 文章id
            article_id = url.split('/')[-1] if url else '无ID'
            # 文章内容
            # 一般在<div class="Post-RichTextContainer">标签内
            content_div = soup.find('div', class_='Post-RichTextContainer')
            if content_div:
                content = content_div.get_text(strip=True)
            else:
                content = '无内容'
            # 处理内容中的图片链接
            img_info = parse_page(response.text, url)
            img_urls = [img['link'] for img in img_info]
            # 文章发布时间和地点
            time_div = soup.find('div', class_='ContentItem-time')
            # <div role="button" tabindex="0" class="ContentItem-time">编辑于 2025-03-18 10:28<!-- -->・北京</div>
            if time_div:
                time_text = time_div.get_text(strip=True)
                # 提取时间和地点
                time_parts = time_text.split('・')
                publish_time = time_parts[0].replace('编辑于', '').strip() if len(time_parts) > 0 else '无时间'
                location = time_parts[1].strip() if len(time_parts) > 1 else '无地点'
            else:
                publish_time = '无时间'
                location = '无地点'
            # 文章作者
            author_div = soup.find('a', class_='UserLink-link')
            if author_div:
                author = author_div.get_text(strip=True)
            else:
                author = '无作者'
            # 作者唯一标识符
            author_id = author_div['href'].split('/')[-1] if author_div and 'href' in author_div.attrs else '无作者ID'
            # 文章支持
            vote_div = soup.find('button', class_='Button VoteButton FEfUrdfMIKpQDJDqkjte')
            if vote_div:
                vote_count = vote_div.get_text(strip=True).split(' ')[1] if '赞同' in vote_div.get_text(strip=True) else '0'
            else:
                vote_count = '0'
            # 文章评论数
            comment_div = soup.find(
                'button', class_='Button BottomActions-CommentBtn FEfUrdfMIKpQDJDqkjte Button--plain Button--withIcon Button--withLabel fEPKGkUK5jyc4fUuT0QP B46v1Ak6Gj5sL2JTS4PY RuuQ6TOh2cRzJr6WlyQp')
            if comment_div:
                comment_count = comment_div.get_text(strip=True).split(' ')[0]
            else:
                comment_count = '0'

            return {
                'article_id': str(article_id),
                'question_id': '',
                'answer_id': '',
                'title': title,
                # 'content': content,
                'img_urls': str(img_urls),
                'publish_time': str(publish_time),
                'location': location,
                'author_id': author_id,
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
            # 获取 问题
            question_div = soup.find('h1', class_='QuestionHeader-title')
            if question_div:
                question = question_div.get_text(strip=True)
            else:
                question = '无问题'
            # 获取回答内容
            content_div = soup.find('div', class_='RichContent RichContent--unescapable')
            if content_div:
                content_div = content_div.find('div', class_='RichContent-inner')
            if content_div:
                content = content_div.get_text(strip=True)
            else:
                content = '无内容'
            # 处理内容中的图片链接
            img_info = parse_page(response.text, url)
            img_urls = [img['link'] for img in img_info]
            # 获取回答作者
            author_div = soup.find('a', class_='UserLink-link')
            if author_div:
                author = author_div.get_text(strip=True)
            else:
                author = '无作者'
            # 作者唯一标识符
            author_id = author_div['href'].split('/')[-1] if author_div and 'href' in author_div.attrs else '无作者ID'
            # 获取回答发布时间
            time_div = soup.find('div', class_='ContentItem-time')
            if time_div:
                publish_time = time_div.get_text(strip=True).replace('发布于', '').strip()
            else:
                publish_time = '无时间'
            # 获取回答支持数
            vote_div = soup.find('button', class_='Button VoteButton FEfUrdfMIKpQDJDqkjte')
            if vote_div:
                vote_count = vote_div.get_text(strip=True).split(' ')[1] if '赞同' in vote_div.get_text(strip=True) else '0'
            else:
                vote_count = '0'
            # 获取回答评论数
            comment_div = soup.find(
                'button', class_='Button ContentItem-action FEfUrdfMIKpQDJDqkjte Button--plain Button--withIcon Button--withLabel fEPKGkUK5jyc4fUuT0QP B46v1Ak6Gj5sL2JTS4PY RuuQ6TOh2cRzJr6WlyQp')
            if comment_div:
                comment_count = comment_div.get_text(strip=True).split(' ')[0]
            else:
                comment_count = '0'
            return {
                'article_id': '',
                'question_id': url.split('/')[-3] if 'question' in url else '',
                'answer_id': url.split('/')[-1],
                'title': question,
                # 'content': content,
                'img_urls': img_urls,
                'publish_time': publish_time,
                'location': '无地点',  # 问答没有地点信息
                'author_id': author_id,
                'vote_count': vote_count,
                'comment_count': comment_count,
            }
        else:
            print(f"请求失败，状态码: {response.status_code}")
            return None


if __name__ == "__main__":
    # url = "https://zhuanlan.zhihu.com/p/30784344002"  # 替换为实际的知乎专栏文章URL
    url = "https://www.zhihu.com/question/19843390/answer/1671468403"
    crawler = Zhihu_BodyCrawler()
    # article_data = crawler.crawl_body_from_articles(url)
    article_data = crawler.crawl_body_from_answers(url)
    if article_data:
        print(json.dumps(article_data, ensure_ascii=False, indent=4))  # indent=4 for pretty print
    else:
        print("未能获取文章数据")
