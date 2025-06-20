import pandas as pd
import re
from bs4 import BeautifulSoup
from datetime import datetime


def extract_mentions(text):
    """
    从抖音评论中提取@用户名
    """
    if pd.isna(text):
        return ""

    # 匹配@用户名格式
    mention_pattern = r'@[^\s@]+'
    mentions = re.findall(mention_pattern, str(text))

    return ', '.join(mentions) if mentions else ""


# 去除异常终止符号
def remove_abnormal_terminators(text):
    """
    移除抖音评论中的异常终止符号
    """
    if pd.isna(text):
        return ""

    # 匹配并移除异常终止符号
    abnormal_pattern = r'[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]'
    cleaned_text = re.sub(abnormal_pattern, '', str(text))

    return cleaned_text.strip()


def extract_hashtags(text):
    """
    从抖音内容中提取话题标签
    """
    if pd.isna(text):
        return ""

    # 匹配#话题#格式
    hashtag_pattern = r'#([^#\s]+)#?'
    hashtags = re.findall(hashtag_pattern, str(text))

    return ', '.join(hashtags) if hashtags else ""


def clean_douyin_mentions(text):
    """
    清除抖音特有的@用户名格式
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
    清除文本中的表情符号
    """
    if pd.isna(text):
        return ""

    # 使用正则表达式匹配 [表情名] 格式的内容并删除
    cleaned_text = re.sub(r'\[.*?\]', '', text)

    # 清理多余的空格
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

    return cleaned_text


def clean_content_pipeline(text):
    """
    内容清理流水线
    """
    if pd.isna(text):
        return ""

    # 清除@用户名
    text = clean_douyin_mentions(text)

    # 移除特殊字符
    text = remove_special_chars(text)

    # 清除表情符号
    text = clean_emoji(text)

    # 移除异常终止符号
    text = remove_abnormal_terminators(text)

    # 最终清理空格
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def save_matched_info_from_meta_data(meta_file, comment_file):
    """
    根据评论数据中的aweme_id，筛选出元数据中匹配的记录
    """
    # 读取CSV文件
    meta_df = pd.read_csv(meta_file, encoding='utf-8', dtype={'aweme_id': str})
    comment_df = pd.read_csv(comment_file, encoding='utf-8', dtype={'aweme_id': str, 'cid': str})

    # 检查是否包含 'aweme_id' 列
    if 'aweme_id' not in meta_df.columns or 'aweme_id' not in comment_df.columns:
        print("CSV文件中不包含 'aweme_id' 列")
        return

    # 获取comment_df中所有的aweme_id
    comment_aweme_ids = set(comment_df['aweme_id'].dropna().astype(str))

    # 匹配到的行
    matched_meta = meta_df[meta_df['aweme_id'].astype(str).isin(comment_aweme_ids)]

    # 保存匹配到的行到新的CSV文件
    print(f"从 {len(meta_df)} 条元数据中匹配到 {len(matched_meta)} 条记录")
    matched_meta_file = meta_file.replace('.csv', '_matched.csv')
    matched_meta.to_csv(matched_meta_file, index=False, encoding='utf-8')
    print(f"匹配到的元数据已保存到：{matched_meta_file}")


def save_matched_info_from_comment_data(comment_file, meta_file):
    """
    根据元数据中的aweme_id，筛选出评论数据中匹配的记录
    """
    # 读取CSV文件
    comment_df = pd.read_csv(comment_file, encoding='utf-8', dtype={'aweme_id': str, 'cid': str})
    meta_df = pd.read_csv(meta_file, encoding='utf-8', dtype={'aweme_id': str})

    # 检查是否包含 'aweme_id' 列
    if 'aweme_id' not in comment_df.columns or 'aweme_id' not in meta_df.columns:
        print("CSV文件中不包含 'aweme_id' 列")
        return

    # 获取meta_df中所有的aweme_id
    meta_aweme_ids = set(meta_df['aweme_id'].dropna().astype(str))

    # 匹配到的行
    matched_comment = comment_df[comment_df['aweme_id'].astype(str).isin(meta_aweme_ids)]

    # 保存匹配到的行到新的CSV文件
    print(f"从 {len(comment_df)} 条评论数据中匹配到 {len(matched_comment)} 条记录")
    matched_comment_file = comment_file.replace('.csv', '_matched.csv')
    matched_comment.to_csv(matched_comment_file, index=False, encoding='utf-8')
    print(f"匹配到的评论数据已保存到：{matched_comment_file}")


def clean_douyin_comments_data(input_file, output_file):
    """
    清理抖音评论数据
    """
    # 读取CSV文件cid,text,aweme_id,create_time,digg_count,status,uid,nickname,reply_id,reply_comment,text_extra,reply_to_reply_id,is_note_comment,ip_label,root_comment_id,level,cotent_type
    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig", dtype={'aweme_id': str, 'cid': str,
                         'uid': str, 'reply_id': str, 'reply_to_reply_id': str, 'root_comment_id': str})
    except:
        try:
            df = pd.read_csv(input_file, encoding="gbk", dtype={'aweme_id': str, 'cid': str,
                             'uid': str, 'reply_id': str, 'reply_to_reply_id': str, 'root_comment_id': str})
        except:
            df = pd.read_csv(input_file, encoding="utf-8", dtype={'aweme_id': str, 'cid': str,
                             'uid': str, 'reply_id': str, 'reply_to_reply_id': str, 'root_comment_id': str})

    # 输出原始条目数
    print(f"原始CSV文件包含 {len(df)} 条记录")

    # 检查是否存在text列
    if 'text' not in df.columns:
        print("错误：CSV文件中没有找到'text'列")
        print(f"现有列名：{df.columns.tolist()}")
        return

    # 提取@用户名并添加到新列
    print("正在提取@用户名...")
    df['mentions'] = df['text'].apply(extract_mentions)

    # 清理text列的内容
    print("正在清理文本内容...")
    df['text'] = df['text'].apply(clean_content_pipeline)

    # 处理数值字段
    numeric_columns = ['digg_count', 'status', 'reply_id', 'level']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)  # 将无法转换的值替换为0

    # 过滤掉text内容长度小于3的记录
    print("正在过滤短内容...")
    # 首先处理NaN值
    df['text'] = df['text'].fillna('')

    # 过滤长度小于3的记录
    original_count = len(df)
    df = df[df['text'].str.len() >= 7]
    filtered_count = len(df)

    print(f"过滤后CSV文件包含 {filtered_count} 条记录")
    print(f"删除了 {original_count - filtered_count} 条短内容记录")

    # 统计包含@用户名的记录数
    mention_count = len(df[df['mentions'] != ''])
    print(f"包含@用户名的记录数：{mention_count}")

    # 保存清理后的数据
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"清理后的数据已保存到：{output_file}")


def clean_douyin_meta_data(input_file, output_file):
    """
    清理抖音元数据
    """
    # 读取CSV文件
    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig", dtype={'aweme_id': str, 'author_uid': str, 'music_id': str})
    except:
        try:
            df = pd.read_csv(input_file, encoding="gbk", dtype={'aweme_id': str, 'author_uid': str, 'music_id': str})
        except:
            df = pd.read_csv(input_file, encoding="utf-8", dtype={'aweme_id': str, 'author_uid': str, 'music_id': str})

    # 输出原始条目数
    print(f"原始CSV文件包含 {len(df)} 条记录")

    # 检查是否存在desc列
    if 'desc' not in df.columns:
        print("错误：CSV文件中没有找到'desc'列")
        print(f"现有列名：{df.columns.tolist()}")
        return

    # 提取话题标签
    print("正在提取话题标签...")
    df['hashtags'] = df['desc'].apply(extract_hashtags)

    # 清理desc列的内容
    print("正在清理描述内容...")
    df['desc'] = df['desc'].apply(clean_content_pipeline)

    # 处理数值字段
    numeric_columns = ['follower_count', 'duration', 'comment_count', 'digg_count', 'share_count', 'collect_count']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 清理desc中的网址
    print("正在清理网址...")
    url_pattern = r'https?://[^\s]+'
    df['desc'] = df['desc'].str.replace(url_pattern, '', regex=True)

    # 去除create_time数值在1743091200之前的记录
    print("正在过滤过早的记录...")
    if 'create_time' in df.columns:
        df = df[df['create_time'] >= 1743091200]
    else:
        print("警告：CSV文件中没有'create_time'列，无法过滤过早的记录")

    # 计算文本长度
    df['text_length'] = df['desc'].apply(lambda x: len(str(x)) if pd.notna(x) else 0)

    # 计算话题标签数量
    df['hashtag_count'] = df['hashtags'].apply(lambda x: len(x.split(', ')) if x else 0)

    # 保存清理后的数据
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"清理后的数据已保存到：{output_file}")


def analyze_douyin_data_structure(file_path):
    """
    分析抖音数据结构
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
    print("分析数据结构...")
    # analyze_douyin_data_structure("douyin_results_final/merged_body.csv")
    # analyze_douyin_data_structure("douyin_results_final/merged_comments.csv")

    # 清理抖音评论数据
    print("\n开始清理抖音评论数据...")
    input_file = "douyin/douyin_results_final/merged_comments.csv"  # 请修改为你的实际输入路径
    output_file = "douyin/douyin_results_final/cleaned_douyin_comments_data.csv"  # 请修改为你的实际输出路径
    clean_douyin_comments_data(input_file, output_file)

    # 清理抖音元数据
    print("\n开始清理抖音元数据...")
    input_file = "douyin/douyin_results_final/merged_body.csv"  # 请修改为你的实际输入路径
    output_file = "douyin/douyin_results_final/cleaned_douyin_meta_data.csv"  # 请修改为你的实际输出路径
    clean_douyin_meta_data(input_file, output_file)

    # 保存匹配信息
    print("\n开始保存匹配信息...")
    save_matched_info_from_meta_data('douyin/douyin_results_final/cleaned_douyin_meta_data.csv',
                                     'douyin/douyin_results_final/cleaned_douyin_comments_data.csv')
    save_matched_info_from_comment_data('douyin/douyin_results_final/cleaned_douyin_comments_data.csv',
                                        'douyin/douyin_results_final/cleaned_douyin_meta_data_matched.csv')
