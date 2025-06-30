import pandas as pd
import re
import os
from datetime import datetime


def extract_image_urls(text):
    """
    从小红书内容中提取图片URL (如果有的话)
    """
    if pd.isna(text):
        return ""

    # 小红书可能包含的图片链接格式
    # 根据实际数据调整正则表达式
    img_patterns = [
        r'https?://[^\s]*\.(jpg|jpeg|png|gif|webp)',
        r'https?://sns-img-[^\s]*\.xhscdn\.com/[^\s]*',
        r'https?://ci\.xiaohongshu\.com/[^\s]*'
    ]

    all_urls = []
    for pattern in img_patterns:
        urls = re.findall(pattern, str(text), re.IGNORECASE)
        if isinstance(urls[0], tuple) if urls else False:
            urls = [url[0] for url in urls]  # 提取元组中的第一个元素
        all_urls.extend(urls)

    return ', '.join(list(set(all_urls))) if all_urls else ""


def extract_hashtags(text):
    """
    从小红书内容中提取话题标签
    """
    if pd.isna(text):
        return ""

    # 小红书话题格式：#开车要小心[话题]# #生命安全高于一切[话题]# #小米汽车[话题]#
    # 匹配 #内容[话题]# 格式
    hashtag_pattern = r'#([^#]+?)\[话题\]#'
    hashtags = re.findall(hashtag_pattern, str(text))

    # 如果没有找到[话题]格式，尝试匹配普通的#话题#格式
    if not hashtags:
        simple_pattern = r'#([^#\s]+)#?'
        hashtags = re.findall(simple_pattern, str(text))

    # 清理提取到的话题（去除多余空格）
    cleaned_hashtags = [tag.strip() for tag in hashtags if tag.strip()]

    return ', '.join(cleaned_hashtags) if cleaned_hashtags else ""


def extract_mentions(text):
    """
    从小红书内容中提取@用户名
    """
    if pd.isna(text):
        return ""

    # 匹配@用户名格式
    mention_pattern = r'@[^\s@]+'
    mentions = re.findall(mention_pattern, str(text))

    return ', '.join(mentions) if mentions else ""


def clean_xhs_mentions(text):
    """
    清除小红书特有的@用户名格式
    """
    if pd.isna(text):
        return ""

    # 匹配@开头的用户名
    pattern = r'@[^\s@]+\s*'
    cleaned_text = re.sub(pattern, '', text)

    # 清理多余的空格
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

    return cleaned_text


def remove_special_chars(text):
    """
    移除特殊字符和不可见字符
    """
    if pd.isna(text):
        return ""

    # 去除零宽字符
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]', '', text)

    # 去除异常的emoji组合
    text = re.sub(r'[\ufe00-\ufe0f]', '', text)  # 变体选择符
    text = re.sub(r'[\u200d]', '', text)  # 零宽连接符

    # 去除私用区字符
    text = re.sub(r'[\ue000-\uf8ff\U000f0000-\U000ffffd\U00100000-\U0010fffd]', '', text)

    return text


def clean_emoji(text):
    """
    清除文本中的表情符号（保留小红书特有格式）
    """
    if pd.isna(text):
        return ""

    # 小红书有特殊的表情格式如[笑哭R]，可以选择保留或删除
    # 这里删除所有[]格式的表情
    cleaned_text = re.sub(r'\[.*?\]', '', text)

    # 清理多余的空格
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

    return cleaned_text


def remove_urls_from_text(text):
    """
    从文本中移除URL，返回清理后的文本
    """
    if pd.isna(text):
        return ""

    # 移除各种URL格式
    url_patterns = [
        r'https?://[^\s]+',
        r'www\.[^\s]+',
        r'[^\s]+\.com[^\s]*'
    ]

    cleaned_text = str(text)
    for pattern in url_patterns:
        cleaned_text = re.sub(pattern, '', cleaned_text)

    # 清理多余的空格
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

    return cleaned_text


def remove_hashtags(text):
    """
    从文本中移除各种格式的话题标签（全面版本）
    """
    if pd.isna(text):
        return ""

    text = str(text)

    # 1. 移除小红书特有的话题格式：#内容[话题]#
    text = re.sub(r'#[^#]+?\[话题\]#', '', text)

    # 2. 移除普通的#话题#格式（有结束#号）
    text = re.sub(r'#[^#\s]+#', '', text)

    # 3. 移除#话题格式（无结束#号，到空格或句尾结束）
    text = re.sub(r'#[^\s#]+(?=\s|$)', '', text)

    # 4. 移除可能遗漏的单独#号
    text = re.sub(r'#+', '', text)

    # 5. 清理多余的空格和换行符
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def clean_content_pipeline(text):
    """
    内容清理流水线
    """
    if pd.isna(text):
        return ""

    # 移除URL
    text = remove_urls_from_text(text)

    # 移除话题标签
    text = remove_hashtags(text)

    # 清除@用户名
    text = clean_xhs_mentions(text)

    # 移除特殊字符
    text = remove_special_chars(text)

    # 清除表情符号
    text = clean_emoji(text)

    # 最终清理空格
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def convert_timestamp(timestamp):
    """
    转换小红书时间戳为秒级时间戳
    """
    try:
        if pd.isna(timestamp):
            return None
        # 小红书时间戳通常是13位毫秒级,转成秒级
        if len(str(timestamp)) == 13:
            timestamp = int(timestamp) / 1000
        elif len(str(timestamp)) == 10:
            timestamp = int(timestamp)
        else:
            return None
        return int(timestamp)
    except ValueError:
        return None
    except:
        return None


def save_matched_info_from_meta_data(content_file, comment_file):
    """
    根据评论数据中的note_id，筛选出内容数据中匹配的记录
    """
    # 读取CSV文件
    content_df = pd.read_csv(content_file, encoding='utf-8', dtype={'note_id': str})
    comment_df = pd.read_csv(comment_file, encoding='utf-8', dtype={'note_id': str, 'comment_id': str})

    # 检查是否包含 'note_id' 列
    if 'note_id' not in content_df.columns or 'note_id' not in comment_df.columns:
        print("CSV文件中不包含 'note_id' 列")
        return

    # 获取comment_df中所有的note_id
    comment_note_ids = set(comment_df['note_id'].dropna().astype(str))

    # 匹配到的行
    matched_content = content_df[content_df['note_id'].astype(str).isin(comment_note_ids)]

    # 保存匹配到的行到新的CSV文件
    matched_content_file = content_file.replace('.csv', '_matched.csv')
    matched_content.to_csv(matched_content_file, index=False, encoding='utf-8')

    print(f"从 {len(content_df)} 条内容数据中匹配到 {len(matched_content)} 条记录")
    print(f"匹配结果已保存到：{matched_content_file}")


def save_matched_info_from_comment_data(comment_file, content_file):
    """
    根据内容数据中的note_id，筛选出评论数据中匹配的记录
    """
    # 读取CSV文件
    comment_df = pd.read_csv(comment_file, encoding='utf-8', dtype={'note_id': str, 'comment_id': str})
    content_df = pd.read_csv(content_file, encoding='utf-8', dtype={'note_id': str})

    # 检查是否包含 'note_id' 列
    if 'note_id' not in comment_df.columns or 'note_id' not in content_df.columns:
        print("CSV文件中不包含 'note_id' 列")
        return

    # 获取content_df中所有的note_id
    content_note_ids = set(content_df['note_id'].dropna().astype(str))

    # 匹配到的行
    matched_comment = comment_df[comment_df['note_id'].astype(str).isin(content_note_ids)]

    # 保存匹配到的行到新的CSV文件
    matched_comment_file = comment_file.replace('.csv', '_matched.csv')
    matched_comment.to_csv(matched_comment_file, index=False, encoding='utf-8')

    print(f"从 {len(comment_df)} 条评论数据中匹配到 {len(matched_comment)} 条记录")
    print(f"匹配结果已保存到：{matched_comment_file}")


def clean_xhs_comments_data(input_file, output_file):
    """
    清理小红书评论数据
    """
    # 读取CSV文件
    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig", dtype={'note_id': str, 'comment_id': str, 'user_id': str, 'parent_comment_id': str})
    except:
        try:
            df = pd.read_csv(input_file, encoding="gbk", dtype={'note_id': str, 'comment_id': str, 'user_id': str, 'parent_comment_id': str})
        except:
            df = pd.read_csv(input_file, encoding="utf-8", dtype={'note_id': str, 'comment_id': str, 'user_id': str, 'parent_comment_id': str})

    # 输出原始条目数
    print(f"原始CSV文件包含 {len(df)} 条记录")

    # 检查是否存在content列
    if 'content' not in df.columns:
        print("错误：CSV文件中没有找到'content'列")
        print(f"现有列名：{df.columns.tolist()}")
        return

    # 提取@用户名
    print("正在提取@用户名...")
    df['mentions'] = df['content'].apply(extract_mentions)

    # 清理content列的内容
    print("正在清理评论内容...")
    df['content'] = df['content'].apply(clean_content_pipeline)

    # 转换时间戳
    if 'create_time' in df.columns:
        print("正在转换时间戳...")
        df['create_time'] = df['create_time'].apply(convert_timestamp)

    # 处理数值字段
    numeric_columns = ['like_count', 'sub_comment_count', 'parent_comment_id']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 处理ip_location列
    if 'ip_location' in df.columns:
        print("正在处理ip_location列...")
        # 处理ip_location, 把空的值填充为'未知'
        df['ip_location'] = df['ip_location'].fillna('未知')
        # 处理ip_location中的特殊字符
        df['ip_location'] = df['ip_location'].apply(lambda x: re.sub(r'[^\w\s]', '', str(x)))
        # 清理多余的空格
        df['ip_location'] = df['ip_location'].apply(lambda x: ' '.join(x.split()))

    # 过滤掉content内容长度小于7的记录
    print("正在过滤短内容...")
    # 首先处理NaN值
    df['content'] = df['content'].fillna('')

    # 过滤长度小于7的记录
    original_count = len(df)
    df = df[df['content'].str.len() >= 7]
    filtered_count = len(df)

    print(f"过滤后CSV文件包含 {filtered_count} 条记录")
    print(f"删除了 {original_count - filtered_count} 条短内容记录")

    # 统计包含@用户名的记录数
    mention_count = len(df[df['mentions'] != ''])
    print(f"包含@用户名的记录数：{mention_count}")

    # 保存清理后的数据
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"清理后的数据已保存到：{output_file}")


def clean_xhs_content_data(input_file, output_file):
    """
    清理小红书内容数据
    """
    # 读取CSV文件
    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig", dtype={'note_id': str, 'user_id': str})
    except:
        try:
            df = pd.read_csv(input_file, encoding="gbk", dtype={'note_id': str, 'user_id': str})
        except:
            df = pd.read_csv(input_file, encoding="utf-8", dtype={'note_id': str, 'user_id': str})

    # 输出原始条目数
    print(f"原始CSV文件包含 {len(df)} 条记录")

    # 检查需要清理的列
    columns_to_clean = []
    if 'desc' in df.columns:
        columns_to_clean.append('desc')
    if 'title' in df.columns:
        columns_to_clean.append('title')

    if not columns_to_clean:
        print("错误：CSV文件中没有找到需要清理的文本列（desc/title）")
        print(f"现有列名：{df.columns.tolist()}")
        return

    # 提取话题标签和@用户名
    for col in columns_to_clean:
        if col in df.columns:
            print(f"正在从{col}列提取话题标签...")
            df[f'{col}_hashtags'] = df[col].apply(extract_hashtags)

            print(f"正在从{col}列提取@用户名...")
            df[f'{col}_mentions'] = df[col].apply(extract_mentions)

    # 清理文本列的内容
    print(f"正在清理文本内容，涉及列：{columns_to_clean}")
    for col in columns_to_clean:
        df[col] = df[col].apply(clean_content_pipeline)

    # 转换时间戳
    time_columns = ['time', 'last_update_time', 'last_modify_ts']
    for col in time_columns:
        if col in df.columns:
            print(f"正在转换{col}时间戳...")
            df[col] = df[col].apply(convert_timestamp)

    # 筛除time数值在1743091200以前的记录
    if 'time' in df.columns:
        print("正在筛除time数值在1743091200以前的记录...")
        df = df[df['time'] >= 1743091200]
    else:
        print("警告：CSV文件中没有'time'列，无法进行时间筛选")

    # 处理数值字段
    numeric_columns = ['liked_count', 'collected_count', 'comment_count', 'share_count']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 处理ip_location列
    if 'ip_location' in df.columns:
        print("正在处理ip_location列...")
        # 处理ip_location, 把空的值填充为'未知'
        df['ip_location'] = df['ip_location'].fillna('未知')
        # 处理ip_location中的特殊字符
        df['ip_location'] = df['ip_location'].apply(lambda x: re.sub(r'[^\w\s]', '', str(x)))
        # 清理多余的空格
        df['ip_location'] = df['ip_location'].apply(lambda x: ' '.join(x.split()))

    # 计算文本长度
    for col in columns_to_clean:
        df[f'{col}_length'] = df[col].apply(lambda x: len(str(x)) if pd.notna(x) else 0)

    # 保存清理后的数据
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"清理后的数据已保存到：{output_file}")


def analyze_xhs_data_structure(file_path):
    """
    分析小红书数据结构
    """
    try:
        df = pd.read_csv(file_path, encoding="utf-8-sig", nrows=5)
    except:
        try:
            df = pd.read_csv(file_path, encoding="gbk", nrows=5)
        except:
            df = pd.read_csv(file_path, encoding="utf-8", nrows=5)

    print(f"文件：{file_path}")
    print(f"列名：{df.columns.tolist()}")
    print(f"数据类型：")
    for col in df.columns:
        print(f"  {col}: {df[col].dtype}")
    print(f"前几行数据示例：")
    print(df.head())
    print("-" * 50)


if __name__ == "__main__":

    # 分析数据结构（可选）
    # print("分析数据结构...")
    # analyze_xhs_data_structure("xhs_results_final/xhs_content.csv")
    # analyze_xhs_data_structure("xhs_results_final/xhs_comments.csv")

    # 清理小红书评论数据
    print("\n开始清理小红书评论数据...")
    input_file = "xhs/xhs_results_final/comments_ds.csv"  # 请修改为你的实际输入路径
    output_file = "xhs/xhs_results_final/cleaned_xhs_comments_ds.csv"  # 请修改为你的实际输出路径
    clean_xhs_comments_data(input_file, output_file)

    # 清理小红书内容数据
    print("\n开始清理小红书内容数据...")
    input_file = "xhs/xhs_results_final/cleaned_xhs_content_data_matched_stance_sentiment_intent.csv"  # 请修改为你的实际输入路径
    output_file = "xhs/xhs_results_final/cleaned_xhs_content_data.csv"  # 请修改为你的实际输出路径
    clean_xhs_content_data(input_file, output_file)

    # 保存匹配信息;';''';''''
    print("\n开始保存匹配信息...")
    save_matched_info_from_meta_data('xhs/xhs_results_final/cleaned_xhs_content_data.csv',
                                     'xhs/xhs_results_final/cleaned_xhs_comments_ds.csv')
    save_matched_info_from_comment_data('xhs/xhs_results_final/cleaned_xhs_comments_ds.csv',
                                        'xhs/xhs_results_final/cleaned_xhs_content_data_matched.csv')

    # rename
    # comment_id,create_time,ip_location,note_id,content,user_id,nickname,avatar,sub_comment_count,pictures,parent_comment_id,last_modify_ts,like_count,mentions,ds_stance,ds_sentiment,ds_intent
    # note_id,type,title,desc,video_url,time,last_update_time,user_id,nickname,avatar,liked_count,collected_count,comment_count,share_count,ip_location,image_list,tag_list,last_modify_ts,note_url,source_keyword,xsec_token,desc_hashtags,desc_mentions,title_hashtags,title_mentions,time_readable,last_update_time_readable,last_modify_ts_readable,desc_length,title_length,multimodal_stance,multimodal_sentiment,multimodal_intent

    input_file = "xhs/xhs_results_final/cleaned_xhs_comments_ds_matched.csv"
    output_file = "xhs/xhs_results_final/cleaned_xhs_comments_ds_renamed.csv"

    df = pd.read_csv(input_file, encoding="utf-8-sig", dtype=str)

    # 重命名列
    df.rename(columns={'note_id': 'article_id',
                       'create_time': 'created_time',
                       'ip_location': 'location',
                       'user_id': 'uid',
                       'nickname': 'username',
                       'sub_comment_count': 'child_comment_count',
                       'pictures': 'img_urls',
                       'ds_stance': 'stance',
                       'ds_sentiment': 'sentiment',
                       'ds_intent': 'intent'}, inplace=True)

    # 输出output文件
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"重命名后的数据已保存到：{output_file}")

    input_file = "xhs/xhs_results_final/cleaned_xhs_content_data_matched.csv"
    output_file = "xhs/xhs_results_final/cleaned_xhs_content_data_renamed.csv"

    df = pd.read_csv(input_file, encoding="utf-8-sig", dtype=str)
    # 重命名列
    df.rename(columns={'note_id': 'article_id',
                       'desc': 'content',
                       'video_url': 'video_urls',
                       'time': 'created_time',
                       'last_update_time': 'updated_time',
                       'user_id': 'uid',
                       'nickname': 'username',
                       'liked_count': 'like_count',
                       'ip_location': 'location',
                       'image_list': 'img_urls',
                       'multimodal_stance': 'stance',
                       'multimodal_sentiment': 'sentiment',
                       'multimodal_intent': 'intent'}, inplace=True)
    # 输出output文件
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"重命名后的数据已保存到：{output_file}")
