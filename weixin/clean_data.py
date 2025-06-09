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


if __name__ == "__main__":

    # 清洗评论内容
    # 读取json
    with open('weixin/weixin_final_results/weixin_comments.json', 'r', encoding='utf-8') as f:
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
    df_comments.to_csv('weixin/weixin_final_results/weixin_comments_clean.csv',
                       index=False, encoding='utf-8-sig')

    # -----—------------------------------------------------

    # 清洗文章内容
    with open('weixin/weixin_final_results/weixin_articles.json', 'r', encoding='utf-8') as f:
        articles = json.load(f)

    # 清洗content
    for art in articles:
        art['content'] = clean_article_content(art.get('content', ''))

    # 转为DataFrame
    df_articles = pd.DataFrame(articles)
    df_articles.to_csv('weixin/weixin_final_results/weixin_articles_clean.csv',
                       index=False, encoding='utf-8-sig')

    # # 读取文章内容csv
    # df_articles = pd.read_csv(
    #     'weixin/weixin_final_results/weixin_articles_clean.csv', encoding='utf-8-sig')
    # # 看看第一行数据以及types
    # print(df_articles['img_urls'].iloc[0])
    # print(type(df_articles['img_urls'].iloc[0]))
