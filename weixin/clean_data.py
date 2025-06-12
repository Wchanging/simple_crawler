import json
import pandas as pd
import re


def clean_article_content(text):
    if not isinstance(text, str):
        return ""
    # 去除HTML标签
    text = re.sub(r'<[^>]+>', '', text)
    # 替换HTML实体
    text = text.replace('&nbsp;', ' ')
    # 去除多余空白符
    text = re.sub(r'\s+', ' ', text)
    # 去除首尾空格
    return text.strip()


def clean_comment_content(text):
    if not isinstance(text, str):
        return ""
    # 去除HTML标签
    text = re.sub(r'<[^>]+>', '', text)
    # 替换HTML实体
    text = text.replace('&nbsp;', ' ')
    # 去除表情符号（如 [微笑]、[捂脸] 等）
    text = re.sub(r'\[[^\]]+\]', '', text)
    # 去除多余空白符
    text = re.sub(r'\s+', ' ', text)
    # 去除首尾空格
    return text.strip()


def clean_articles(file_path, output_path):
    """
    清洗文章内容并保存到CSV文件。

    :param file_path: 输入的JSON文件路径
    :param output_path: 输出的CSV文件路径
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        articles = json.load(f)

    # 清洗content
    for art in articles:
        art['content'] = clean_article_content(art.get('content', ''))

    # 转为DataFrame
    df_articles = pd.DataFrame(articles)
    df_articles.to_csv(output_path, index=False, encoding='utf-8-sig')


def clean_comments(file_path, output_path):
    """
    清洗评论内容并保存到CSV文件。

    :param file_path: 输入的JSON文件路径
    :param output_path: 输出的CSV文件路径
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        comments = json.load(f)

    # 清洗content
    for c in comments:
        c['content'] = clean_comment_content(c.get('content', ''))

    # 去重（content + content_id）
    seen = set()
    unique_comments = []
    for c in comments:
        key = (c['content'], c['content_id'])
        if key not in seen:
            seen.add(key)
            unique_comments.append(c)

    # 转为DataFrame
    df_comments = pd.DataFrame(unique_comments)
    df_comments.to_csv(output_path, index=False, encoding='utf-8-sig')


def delet_articles_without_comments(file_path_articles, file_path_comments, output_path):
    """
    删除没有评论的文章。

    :param file_path_articles: 文章CSV文件路径
    :param file_path_comments: 评论CSV文件路径
    :output_path: 输出的CSV文件路径
    """
    df_articles = pd.read_csv(file_path_articles, encoding='utf-8-sig')
    df_comments = pd.read_csv(file_path_comments, encoding='utf-8-sig')

    # 获取有评论的文章ID
    comment_article_ids = set(df_comments['url'])

    # 过滤出有评论的文章
    df_filtered_articles = df_articles[df_articles['url'].isin(comment_article_ids)]
    df_filtered_articles.to_csv(output_path, index=False, encoding='utf-8-sig')


if __name__ == "__main__":

    # # 清洗评论内容
    # clean_comments('weixin/weixin_final_results/weixin_comments.json',
    #                'weixin/weixin_final_results/weixin_comments_clean.csv')
    # # -----—------------------------------------------------
    # # 清洗文章内容
    # clean_articles('weixin/weixin_final_results/weixin_articles.json',
    #                'weixin/weixin_final_results/weixin_articles_clean.csv')

    # 删除没有评论的文章
    delet_articles_without_comments(
        'weixin/weixin_final_results/weixin_articles_clean.csv',
        'weixin/weixin_final_results/weixin_comments_clean.csv',
        'weixin/weixin_final_results/weixin_articles_with_comments.csv'
    )

    # # 读取文章内容csv
    # df_articles = pd.read_csv(
    #     'weixin/weixin_final_results/weixin_articles_clean.csv', encoding='utf-8-sig')
    # # 看看第一行数据以及types
    # print(df_articles['img_urls'].iloc[0])
    # print(type(df_articles['img_urls'].iloc[0]))
