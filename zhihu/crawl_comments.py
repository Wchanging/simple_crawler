import requests
import json
from datetime import datetime
import csv
import time
import os


class ZhiHu_CommentCrawler:
    def __init__(self):
        self.headers = self.get_headers()
        self.comments_list = []

    def get_headers(self, open_path='zhihu/zhihu_cookie_.json'):
        with open(open_path, 'r', encoding='utf-8') as f:
            headers = json.load(f)
        return headers

    def clean_comment_list(self):
        self.comments_list = []

    def save_comments_to_csv(self, filename='comments.csv', is_append=True):
        mode = 'a' if is_append else 'w'
        with open(filename, mode, newline='', encoding='utf-8') as csvfile:
            fieldnames = ['article_id', 'answer_id', 'question_id', 'comment_id', 'super_comment_id',
                          'content', 'like_count', 'dislike_count', 'author', 'author_name', 'gender',
                          'created_time', 'created_area',
                          'child_comment_count', 'is_article']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            # 如果文件本身有内容，则不写入表头
            if not is_append or os.stat(filename).st_size == 0:
                writer.writeheader()
            for comment in self.comments_list:
                writer.writerow(comment)
        print(f'Comments saved to {filename}')

    def change_url(self, url):
        # https://zhuanlan.zhihu.com/p/1891878484755871157
        # https://www.zhihu.com/question/1890393113299775650/answer/1890538177489515849
        # 有以上两种形式的url —— 知乎专栏和知乎问答
        # 专栏转换成https://www.zhihu.com/api/v4/comment_v5/articles/{article_id}/root_comment?order_by=score&limit=20&offset=
        # 问答转换成https://www.zhihu.com/api/v4/comment_v5/answers/{answer_id}/root_comment?order_by=score&limit=20&offset=
        if 'zhuanlan.zhihu.com' in url:
            # 专栏文章
            article_id = url.split('/')[-1]
            new_url = f'https://www.zhihu.com/api/v4/comment_v5/articles/{article_id}/root_comment?order_by=score&limit=20&offset='
            return new_url, article_id, '', ''
        elif 'zhihu.com/question/' in url and 'answer' in url:
            # 问答文章
            question_id = url.split('/')[-3]
            answer_id = url.split('/')[-1]
            new_url = f'https://www.zhihu.com/api/v4/answers/{answer_id}/root_comments?order=normal&limit=20&offset=0&status=open'
            return new_url, '', question_id, answer_id
        else:
            print('URL格式不正确，请提供知乎专栏或问答的链接')
            return '', '', '', ''

    def crawl_comments_from_articles(self, url, article_id=''):
        finish = 0
        url_ = url
        while (finish < 2):
            response = requests.get(url_, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                # 先判断是否为起始页
                if_start = data.get('paging', {}).get('is_start', False)
                if_end = data.get('paging', {}).get('is_end', False)
                url_ = data.get('paging', {}).get('next', '')
                if if_start or if_end:
                    finish += 1
                    if finish >= 2:
                        print("评论抽取完成")
                        break
                comments = data.get('data', [])
                if data == []:
                    print('This url has no more comments to fetch.')
                    break
                for comment in comments:
                    comment_id = comment.get('id')
                    content = comment.get('content')
                    author = comment.get('author', {}).get('id')
                    author_name = comment.get('author', {}).get('name')
                    created_time = comment.get('created_time')
                    created_area = comment.get('comment_tag', [])
                    like_count = comment.get('like_count', 0)
                    dislike_count = comment.get('dislike_count', 0)
                    gender = comment.get('author', {}).get('gender', -1)
                    if created_area:
                        created_area = created_area[0].get('text', '未知')
                    else:
                        created_area = '未知'
                    child_comment_count = comment.get('child_comment_count', 0)
                    self.comments_list.append({
                        'article_id': str(article_id),
                        'answer_id': '',
                        'question_id': '',
                        'comment_id': str(comment_id),
                        'super_comment_id': '',
                        'content': content,
                        'like_count': like_count,
                        'dislike_count': dislike_count,
                        'author': author,
                        'author_name': author_name,
                        'gender': gender,
                        'created_time': created_time,
                        'created_area': created_area,
                        'child_comment_count': child_comment_count,
                        'is_article': article_id != ''
                    })
                    # print(type(child_comment_count), child_comment_count)
                    if child_comment_count > 0:
                        child_url = f'https://www.zhihu.com/api/v4/comment_v5/comment/{comment_id}/child_comment?order_by=ts&limit=20&offset='
                        self.crawl_child_comments(child_url, article_id, question_id='', answer_id='', super_comment_id=comment_id)
            else:
                print(f'Failed to fetch comments, status code: {response.status_code}')
                break
            # 等待，避免请求过快
            print(f'Fetched {len(self.comments_list)} comments so far.')
            print('等待3秒后继续爬取评论')
            time.sleep(3)

    def crawl_child_comments(self, url, article_id='', question_id='', answer_id='', super_comment_id=''):
        finish = 0
        url_ = url
        while (finish < 2):
            response = requests.get(url_, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                # print(data)
                # print(f'正在爬取子评论，当前url: {url_}')
                # 先判断是否为起始页
                if_start = data.get('paging', {}).get('is_start', False)
                if_end = data.get('paging', {}).get('is_end', False)
                url_ = data.get('paging', {}).get('next', '')
                if if_end or if_start:
                    finish += 1
                    if finish >= 2:
                        print("子评论抽取完成")
                        break
                comments = data.get('data', [])
                if data == [] or comments == []:
                    print('This url has no more comments to fetch.')
                    break
                for comment in comments:
                    comment_id = comment.get('id')
                    content = comment.get('content')
                    author = comment.get('author', {}).get('id')
                    author_name = comment.get('author', {}).get('name')
                    created_time = comment.get('created_time')
                    created_area = comment.get('comment_tag', [])
                    like_count = comment.get('like_count', 0)
                    dislike_count = comment.get('dislike_count', 0)
                    gender = comment.get('author', {}).get('gender', -1)
                    if created_area:
                        created_area = created_area[0].get('text', '未知')
                    else:
                        created_area = '未知'
                    child_comment_count = comment.get('child_comment_count', 0)
                    self.comments_list.append({
                        'article_id': str(article_id),
                        'answer_id': str(answer_id),
                        'question_id': str(question_id),
                        'comment_id': str(comment_id),
                        'super_comment_id': str(super_comment_id),
                        'content': content,
                        'like_count': like_count,
                        'dislike_count': dislike_count,
                        'author': author,
                        'author_name': author_name,
                        'gender': gender,
                        'created_time': datetime.fromtimestamp(created_time).strftime('%Y-%m-%d %H:%M:%S'),
                        'created_area': created_area,
                        'child_comment_count': child_comment_count,
                        'is_article': article_id != ''
                    })
            else:
                print(f'Failed to fetch child comments, status code: {response.status_code}')
                break
            # 等待，避免请求过快
            print(f'Fetched {len(self.comments_list)} comments so far.')
            print('等待2秒后继续爬取评论')
            time.sleep(2)

    def crawl_comments_from_answers(self, url, question_id='', answer_id=''):
        # 处理知乎问答的评论
        finish = 0
        url_ = url
        while (finish < 10):
            finish += 1
            response = requests.get(url_, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                url_ = data.get('paging', {}).get('next', '')
                comments = data.get('data', [])
                if data == [] or comments == []:
                    print('This url has no more comments to fetch.')
                    break
                for comment in comments:
                    comment_id = comment.get('id')
                    content = comment.get('content')
                    author = comment.get('author', {}).get('member', {}).get('id')
                    author_name = comment.get('author', {}).get('member', {}).get('name')
                    created_time = comment.get('created_time')
                    created_area = comment.get('comment_tag', [])
                    like_count = comment.get('vote_count', 0)
                    dislike_count = comment.get('dislike_count', 0)
                    gender = comment.get('author', {}).get('member', {}).get('gender', -1)
                    created_area = comment.get('address_text', '未知')
                    child_comment_count = comment.get('child_comment_count', 0)
                    self.comments_list.append({
                        'article_id': '',
                        'answer_id': str(answer_id),
                        'question_id': str(question_id),
                        'comment_id': str(comment_id),
                        'super_comment_id': '',
                        'content': content,
                        'like_count': like_count,
                        'dislike_count': dislike_count,
                        'author': author,
                        'author_name': author_name,
                        'gender': gender,
                        'created_time': datetime.fromtimestamp(created_time).strftime('%Y-%m-%d %H:%M:%S'),
                        'created_area': created_area,
                        'child_comment_count': child_comment_count,
                        'is_article': article_id != ''
                    })

                    # print(type(child_comment_count), child_comment_count)
                    if child_comment_count > 0:
                        child_url = f'https://www.zhihu.com/api/v4/comment_v5/comment/{comment_id}/child_comment?order_by=ts&limit=20&offset='
                        self.crawl_child_comments(child_url, article_id='', question_id=question_id, answer_id=answer_id, super_comment_id=comment_id)

            else:
                print(f'Failed to fetch comments, status code: {response.status_code}')
                break
            # 等待，避免请求过快
            print(f'Fetched {len(self.comments_list)} comments so far.')
            print('等待3秒后继续爬取评论')
            time.sleep(3)


if __name__ == "__main__":
    crawler = ZhiHu_CommentCrawler()
    # # url = f'https://www.zhihu.com/api/v4/comment_v5/articles/1891878484755871157/root_comment?order_by=score&limit=20&offset='
    # url = 'https://zhuanlan.zhihu.com/p/1890520461999326022'
    # url = 'https://www.zhihu.com/question/1891043594355327452/answer/1891061130014733611'
    # url = 'https://www.zhihu.com/question/1890388088095728047/answer/1890396693389869855'
    # url = 'https://www.zhihu.com/question/605855963/answer/3561012511'
    # new_url, article_id, question_id, answer_id = crawler.change_url(url)
    # print(new_url)
    # # crawler.crawl_comments_from_articles(new_url, article_id)
    # crawler.crawl_comments_from_answers(new_url, question_id, answer_id)
    # crawler.save_comments_to_csv('zhihu/comments_test.csv', is_append=True)

    # 读取url
    with open('zhihu/filtered_urls.txt', 'r', encoding='utf-8') as f:
        urls = f.readlines()
    urls = [url.strip() for url in urls if url.strip()]  # 去除空行
    for i in range(len(urls)):
        url = urls[i]
        print(f'正在处理第 {i + 1} 个 URL:')
        try:
            new_url, article_id, question_id, answer_id = crawler.change_url(url)
            print(f'Processing URL: {url}')
            # crawler.crawl_comments_from_articles(new_url, article_id, question_id, answer_id)
            if article_id != '':
                crawler.crawl_comments_from_articles(new_url, article_id)
            elif question_id != '' and answer_id != '':
                crawler.crawl_comments_from_answers(new_url, question_id, answer_id)

            crawler.save_comments_to_csv('zhihu/comments_full_2.csv', is_append=True)
            crawler.clean_comment_list()  # 清空评论列表，准备下一次爬取
            print(f'Finished processing URL: {url}')
            print('等待5秒后继续爬取下一个 URL')
            print('-' * 50)
            time.sleep(5)
        except Exception as e:
            print(f'Error processing URL {url}: {e}')
