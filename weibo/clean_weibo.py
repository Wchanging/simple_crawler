import csv
import pandas as pd
import os
import json
import re


def extract_image_urls(text):
    """
    从文本中提取图片URL
    """
    if pd.isna(text):
        return ""

    # 匹配 http://t.cn/ 开头的链接
    url_pattern = r'http://t\.cn/[A-Za-z0-9]+'
    urls = re.findall(url_pattern, str(text))

    # 返回url列表
    return ', '.join(urls) if urls else ""


def remove_urls_from_text(text):
    """
    从文本中移除URL，返回清理后的文本
    """
    if pd.isna(text):
        return ""

    # 移除 http://t.cn/ 开头的链接
    url_pattern = r'http://t\.cn/[A-Za-z0-9]+'
    cleaned_text = re.sub(url_pattern, '', str(text))

    # 清理多余的空格
    cleaned_text = ' '.join(cleaned_text.split())

    return cleaned_text


def clean_reply_text(text):
    """清理微博文本，去掉回复前缀、多余空格等"""
    if pd.isna(text):
        return ""

    # 去掉"回复@用户名:"的前缀
    text = re.sub(r'^回复@[^：:]+[：:]', '', text)

    # 去掉多余的空格和换行符
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def clean_at_mentions(text):
    """
    清除文本中的@用户名格式
    包括：@蜀郡骑都尉、@苦难生活2025: 等格式
    """
    # 匹配@开头，后跟用户名（可包含中文、英文、数字），可能以冒号结尾
    pattern = r'@[^\s@]+:?\s*'

    # 替换为空字符串
    cleaned_text = re.sub(pattern, '', text)

    # 清理多余的空格
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

    return cleaned_text


def remove_invisible_emojis(text):
    # 去除各种不可见字符和异常表情
    # 去除零宽字符
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]', '', text)

    # 去除异常的emoji组合
    text = re.sub(r'[\ufe00-\ufe0f]', '', text)  # 变体选择符
    text = re.sub(r'[\u200d]', '', text)  # 零宽连接符

    # 去除私用区字符
    text = re.sub(r'[\ue000-\uf8ff\U000f0000-\U000ffffd\U00100000-\U0010fffd]', '', text)

    return text


def remove_weibo_emoji(text):
    # 使用正则表达式匹配 [表情名] 格式的内容并删除
    cleaned_text = re.sub(r'\[.*?\]', '', text)
    return cleaned_text


if __name__ == "__main__":
    # 读取CSV文件
    csv_file = "weibo/weibo_details/weibo_review_data_updated.csv"
    df = pd.read_csv(csv_file, encoding="utf-8-sig")

    # 输出原始条目数
    print(f"原始CSV文件包含 {len(df)} 条记录")

    # 检查是否存在text_raw列
    if 'text_raw' not in df.columns:
        print("错误：CSV文件中没有找到'text_raw'列")
        print(f"现有列名：{df.columns.tolist()}")
        exit()

    # 提取图片URL并添加到新列
    df['img_url'] = df['text_raw'].apply(extract_image_urls)

    # 从text_raw中移除URL（可选，如果你想保持原文本不变，可以注释掉这行）
    df['text_raw'] = df['text_raw'].apply(remove_urls_from_text)

    # 清理微博文本
    df['text_raw'] = df['text_raw'].apply(clean_reply_text)

    # 清除@用户名格式
    df['text_raw'] = df['text_raw'].apply(clean_at_mentions)

    # 去除不可见字符和异常表情
    df['text_raw'] = df['text_raw'].apply(remove_invisible_emojis)
    df['text_raw'] = df['text_raw'].apply(remove_weibo_emoji)

    # 过滤掉text_raw内容长度小于3的记录
    # 首先处理NaN值
    df['text_raw'] = df['text_raw'].fillna('')

    # 过滤长度小于3的记录
    original_count = len(df)
    df = df[df['text_raw'].str.len() >= 3]
    filtered_count = len(df)

    print(f"过滤后CSV文件包含 {filtered_count} 条记录")
    print(f"删除了 {original_count - filtered_count} 条长度小于3的记录")

    # 统计包含图片URL的记录数
    img_count = len(df[df['img_url'] != ''])
    print(f"包含图片URL的记录数：{img_count}")

    # 保存清理后的数据
    output_file = "weibo/weibo_details/cleaned_review_data.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"清理后的数据已保存到：{output_file}")

    # 显示前几行数据作为示例
    print("\n前5行数据示例：")
    print(df[['text_raw', 'img_url']].head())
