import pandas as pd
import os
import json
import csv
import re


# 计算comments_full.csv中总共有多少条评论
def count_comments_in_csv(csv_file):
    if not os.path.exists(csv_file):
        print(f"文件 {csv_file} 不存在")
        return 0

    df = pd.read_csv(csv_file, encoding="utf-8-sig")
    total_comments = len(df)
    # print(df.head())  # 打印前几行数据以检查格式
    print(f"CSV文件 {csv_file} 包含 {total_comments} 条评论")
    return total_comments

# 计算comments_full.csv中有多少article_id，多少answer_id


def count_ids_in_csv(csv_file):
    if not os.path.exists(csv_file):
        print(f"文件 {csv_file} 不存在")
        return 0, 0, 0

    # df = pd.read_csv(csv_file, encoding="utf-8-sig")
    # 有的整数太大了，显示不出来，改成字符串读取
    df = pd.read_csv(csv_file, encoding="utf-8-sig", dtype={'article_id': str, 'answer_id': str, 'question_id': str})

    # 计算不同的article_id
    article_ids = df['article_id'].dropna().unique()
    total_article_ids = len(article_ids)

    # 计算不同的answer_id
    answer_ids = df['answer_id'].dropna().unique()
    total_answer_ids = len(answer_ids)

    # 计算不同的question_id
    question_ids = df['question_id'].dropna().unique()
    total_question_ids = len(question_ids)

    print(f"CSV文件 {csv_file} 包含 {total_article_ids} 个不同的文章ID，{total_answer_ids} 个不同的回答ID，{total_question_ids} 个不同的问题ID")

    return article_ids, answer_ids, question_ids


def change_url(url):
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

# 筛选出zhuhu_urls.txt中有内容在comments_full.csv中的url，注意区分知乎问答和文章


def filter_urls_in_csv(urls_file, csv_file):
    if not os.path.exists(urls_file):
        print(f"文件 {urls_file} 不存在")
        return []

    if not os.path.exists(csv_file):
        print(f"文件 {csv_file} 不存在")
        return []

    # 读取urls_file中的URL
    with open(urls_file, 'r', encoding='utf-8') as f:
        urls = f.readlines()
    urls = [url.strip() for url in urls if url.strip()]  # 去除空行和多余空格
    print(f"从 {urls_file} 中读取到 {len(urls)} 个URL")

    # 读取csv_file中的数据
    df = pd.read_csv(csv_file, encoding="utf-8-sig", dtype={'article_id': str, 'answer_id': str, 'question_id': str, 'comment_id': str})

    # 提取并转换ID为字符串集合，提高查找效率
    article_ids = set(df['article_id'].dropna().astype(str).unique())
    answer_ids = set(df['answer_id'].dropna().astype(str).unique())
    question_ids = set(df['question_id'].dropna().astype(str).unique())

    print(f"从 {csv_file} 中读取到 {len(article_ids)} 个文章ID，{len(answer_ids)} 个回答ID，{len(question_ids)} 个问题ID")

    # 筛选出在csv_file中存在的URL
    filtered_urls = []
    matched_articles = set()
    matched_answers = set()
    matched_questions = set()

    for url in urls:
        new_url, article_id, question_id, answer_id = change_url(url)
        if article_id and article_id in article_ids:
            # 文章类型URL
            filtered_urls.append(url)
            matched_articles.add(article_id)
            # print(f"匹配到文章ID: {article_id}")

        elif question_id and answer_id and (answer_id in answer_ids or question_id in question_ids):
            # 问答类型URL - 只要answer_id或question_id其中一个匹配即可
            filtered_urls.append(url)
            if answer_id in answer_ids:
                matched_answers.add(answer_id)
            if question_id in question_ids:
                matched_questions.add(question_id)
            # print(f"匹配到问答 - 问题ID: {question_id}, 回答ID: {answer_id}")

    print(f"实际匹配到: {len(matched_articles)} 个文章ID，{len(matched_answers)} 个回答ID，{len(matched_questions)} 个问题ID")
    print(f"在 {urls_file} 中找到 {len(filtered_urls)} 个在 {csv_file} 中存在的URL")

    # 保存筛选后的URL到新的文件
    filtered_urls_file = 'zhihu/filtered_urls2.txt'
    with open(filtered_urls_file, 'w', encoding='utf-8') as f:
        for url in filtered_urls:
            f.write(url + '\n')
    print(f"筛选后的URL已保存到 {filtered_urls_file}")

    return filtered_urls


# 在主函数中添加转换功能
if __name__ == "__main__":
    csv_file = 'zhihu/comments_full_2.csv'
    # urls_file = 'zhihu/zhihu_urls.txt'

    # 步骤1: 计算comments_full.csv中的评论数量和ID数量
    print("\n步骤1: 计算评论数量和ID数量")
    # total_comments = count_comments_in_csv(csv_file)

    article_ids, answer_ids, question_ids = count_ids_in_csv(csv_file)

    # print("\n步骤2: 筛选URL")
    # filtered_urls = filter_urls_in_csv(urls_file, csv_file)
    # print(f"筛选后的URL数量: {len(filtered_urls)}")
