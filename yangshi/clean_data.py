"""
央视网文章数据清洗脚本
功能：清洗CSV文件，提取作者、格式化时间、生成唯一ID等
"""
import pandas as pd
import re
import uuid
from datetime import datetime
import os


def clean_text_content(text):
    """
    清洗文本内容，去掉多余回车、杂乱符号等

    Args:
        text: 原始文本

    Returns:
        str: 清洗后的文本
    """
    if pd.isna(text) or text == "":
        return ""

    text = str(text)

    # 去除HTML标签
    text = re.sub(r'<[^>]+>', '', text)

    # 去除HTML实体
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&amp;', '&')
    text = text.replace('&quot;', '"')
    text = text.replace('&#39;', "'")

    # 去除多余的回车和换行符（保留段落间的换行）
    text = re.sub(r'\n\s*\n', '\n', text)  # 多个连续换行变成单个
    text = re.sub(r'\r\n', '\n', text)  # 统一换行符
    text = re.sub(r'\r', '\n', text)

    # 去除零宽字符
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]', '', text)

    # 去除特殊控制字符
    text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', text)

    # 清理多余的空格（不影响段落内的正常空格）
    text = re.sub(r'[ \t]+', ' ', text)  # 多个空格变成单个
    text = re.sub(r' *\n *', '\n', text)  # 去除换行前后的空格

    # 去除首尾空白
    text = text.strip()

    return text


def extract_user_from_publish_info(publish_info):
    """
    从publish_info中提取作者（来源）
    例如: "来源：新华网|  2025年04月25日 16:32" -> "新华网"

    Args:
        publish_info: 发布信息字符串

    Returns:
        str: 作者/来源
    """
    if pd.isna(publish_info) or publish_info == "":
        return ""

    publish_info = str(publish_info)

    # 尝试匹配 "来源：XXX|" 或 "XXX|" 格式
    patterns = [
        r'来源[：:]\s*([^|]+)',  # 来源：新华网
        r'^([^|]+)\|',  # 直接以|分隔
        r'央视新闻',  # 如果包含央视新闻
        r'新华网',  # 如果包含新华网
        r'CCTV',  # 如果包含CCTV
    ]

    for pattern in patterns:
        match = re.search(pattern, publish_info)
        if match:
            if '来源' in pattern or '^' in pattern:
                return match.group(1).strip()
            else:
                return match.group(0).strip()

    # 如果都没匹配到，取第一个|之前的部分
    if '|' in publish_info:
        return publish_info.split('|')[0].strip()

    return publish_info.strip()


def parse_publish_time(time_str):
    """
    解析发布时间，转换为标准格式

    Args:
        time_str: 时间字符串，如 "2025年04月25日 16:32"

    Returns:
        tuple: (格式化时间字符串, 时间戳)
    """
    if pd.isna(time_str) or time_str == "":
        return "", 0

    time_str = str(time_str).strip()

    try:
        # 尝试多种时间格式
        formats = [
            '%Y年%m月%d日 %H:%M:%S',  # 2025年04月25日 16:32:45
            '%Y年%m月%d日 %H:%M',     # 2025年04月25日 16:32
            '%Y-%m-%d %H:%M:%S',      # 2025-04-25 16:32:45
            '%Y-%m-%d %H:%M',         # 2025-04-25 16:32
            '%Y/%m/%d %H:%M:%S',      # 2025/04/25 16:32:45
            '%Y/%m/%d %H:%M',         # 2025/04/25 16:32
        ]

        dt = None
        for fmt in formats:
            try:
                dt = datetime.strptime(time_str, fmt)
                break
            except ValueError:
                continue

        if dt is None:
            # 如果都失败，尝试提取数字
            match = re.search(r'(\d{4})年?[/-]?(\d{1,2})月?[/-]?(\d{1,2})日?\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?', time_str)
            if match:
                year, month, day, hour, minute = match.groups()[:5]
                second = match.group(6) if match.group(6) else '00'
                dt = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))

        if dt:
            # 格式化为标准格式: YYYY-MM-DD HH:MM:SS
            formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
            # 转换为时间戳（秒）
            timestamp = int(dt.timestamp())
            return formatted_time, timestamp

    except Exception as e:
        print(f"解析时间出错 '{time_str}': {e}")

    return time_str, 0


def generate_post_id():
    """
    生成唯一的帖子ID
    格式: post_{6位随机十六进制}

    Returns:
        str: 帖子ID
    """
    return f"post_{uuid.uuid4().hex[:6]}"


def clean_yangshi_data(input_file, output_file=None):
    """
    清洗央视网文章CSV数据

    Args:
        input_file: 输入CSV文件路径
        output_file: 输出CSV文件路径，默认为输入文件名加_cleaned后缀

    Returns:
        pd.DataFrame: 清洗后的数据框
    """
    print(f"开始读取文件: {input_file}")

    # 读取CSV文件
    try:
        df = pd.read_csv(input_file, encoding='utf-8-sig')
    except:
        try:
            df = pd.read_csv(input_file, encoding='gbk')
        except:
            df = pd.read_csv(input_file, encoding='utf-8')

    print(f"原始数据: {len(df)} 条记录")
    print(f"原始列: {list(df.columns)}")

    # 生成唯一帖子ID
    print("\n生成唯一帖子ID...")
    df['post_id'] = [generate_post_id() for _ in range(len(df))]

    # 清洗文本内容
    print("清洗文本内容...")
    if 'content' in df.columns:
        df['content'] = df['content'].apply(clean_text_content)

    if 'title' in df.columns:
        df['title'] = df['title'].apply(clean_text_content)

    if 'subtitle' in df.columns:
        df['subtitle'] = df['subtitle'].apply(clean_text_content)

    # 从publish_info提取作者
    print("提取作者信息...")
    if 'publish_info' in df.columns:
        df['username'] = df['publish_info'].apply(extract_user_from_publish_info)
    else:
        df['username'] = ""

    # 解析并格式化发布时间
    print("格式化发布时间...")
    if 'publish_time' in df.columns:
        time_data = df['publish_time'].apply(parse_publish_time)
        df['created_date'] = time_data.apply(lambda x: x[0])
        df['created_time'] = time_data.apply(lambda x: x[1])
    else:
        df['created_date'] = ""
        df['created_time'] = 0

    # 清理图片URL字符串
    if 'img_urls' in df.columns:
        df['img_urls'] = df['img_urls'].apply(lambda x: str(x) if not pd.isna(x) else "[]")

    df['platform'] = "yangshi"
    df['like_count'] = 0
    df['comment_count'] = 0
    df['share_count'] = 0
    df['uid'] = ""
    df['video_urls'] = "[]"

    # 调整列顺序
    columns_order = [
        'post_id',
        'title',
        'content',
        'created_date',
        'platform',
        'like_count',
        'comment_count',
        'share_count',
        'uid',
        'username',
        'video_urls',
        'img_urls',
        'created_time'
    ]

    # 只保留存在的列
    final_columns = [col for col in columns_order if col in df.columns]
    df = df[final_columns]

    # 删除完全空白的行
    df = df.dropna(how='all')

    # 删除标题和内容都为空的行
    if 'title' in df.columns and 'content' in df.columns:
        df = df[~((df['title'] == "") & (df['content'] == ""))]

    print(f"\n清洗后数据: {len(df)} 条记录")

    # 保存到文件
    if output_file is None:
        # 生成默认输出文件名
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}_cleaned.csv"

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"清洗后的数据已保存到: {output_file}")

    # 显示统计信息
    print(f"\n数据统计:")
    print(f"- 总记录数: {len(df)}")
    print(f"- 有标题的记录: {df['title'].notna().sum() if 'title' in df.columns else 0}")
    print(f"- 有内容的记录: {df['content'].notna().sum() if 'content' in df.columns else 0}")
    print(f"- 有图片的记录: {(df['image_count'] > 0).sum() if 'image_count' in df.columns else 0}")
    print(f"- 有时间戳的记录: {(df['created_time'] > 0).sum() if 'created_time' in df.columns else 0}")

    # 显示几条示例数据
    print(f"\n前3条数据预览:")
    print(df.head(3)[['post_id', 'title', 'username', 'created_time']].to_string())

    return df


def batch_clean_files(file_list, output_dir=None):
    """
    批量清洗多个CSV文件

    Args:
        file_list: CSV文件路径列表
        output_dir: 输出目录，默认为各文件所在目录
    """
    for file_path in file_list:
        print(f"\n{'='*60}")
        print(f"处理文件: {file_path}")
        print(f"{'='*60}")

        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            file_name = os.path.basename(file_path)
            base_name = os.path.splitext(file_name)[0]
            output_file = os.path.join(output_dir, f"{base_name}_cleaned.csv")
        else:
            output_file = None

        try:
            clean_yangshi_data(file_path, output_file)
            print(f"✓ 处理成功")
        except Exception as e:
            print(f"✗ 处理失败: {e}")


if __name__ == "__main__":
    # 单个文件清洗
    input_file = "yangshi/yangshi_articles/all_articles.csv"
    output_file = "yangshi/yangshi_articles/all_articles_cleaned.csv"

    clean_yangshi_data(input_file, output_file)

    # 批量清洗多个文件
    # file_list = [
    #     "yangshi/yangshi_articles/all_articles.csv",
    #     "yangshi/yangshi_articles/all_articles2.csv",
    # ]
    # batch_clean_files(file_list, output_dir="yangshi/yangshi_articles_cleaned")
