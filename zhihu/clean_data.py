import pandas as pd
import re
from bs4 import BeautifulSoup


def extract_image_urls(text):
    """
    从知乎评论HTML中提取图片URL
    """
    if pd.isna(text):
        return ""

    # 匹配知乎图片链接
    # <a href="https://pic3.zhimg.com/v2-ef7c533a00c765104e2388034472f8b6_qhd.jpeg" class="comment_img" data-width="1200" data-height="2670">查看图片</a>
    img_pattern = r'<a[^>]*href=["\']?(https://pic[^.]*\.zhimg\.com/[^"\'>\s]*)["\']?[^>]*class=["\']comment_img["\'][^>]*>.*?</a>'
    urls = re.findall(img_pattern, str(text), re.IGNORECASE)

    # 也匹配直接的图片链接
    direct_img_pattern = r'https://pic[^.]*\.zhimg\.com/[^\s<>"\']*'
    direct_urls = re.findall(direct_img_pattern, str(text))

    # 合并去重
    all_urls = list(set(urls + direct_urls))

    return ", ".join(all_urls) if all_urls else ""


def remove_html_tags(text):
    """
    移除HTML标签，保留纯文本内容
    """
    if pd.isna(text):
        return ""

    try:
        # 使用BeautifulSoup解析HTML
        soup = BeautifulSoup(str(text), "html.parser")

        # 提取纯文本
        clean_text = soup.get_text()

        # 清理多余的空格和换行符
        clean_text = re.sub(r"\s+", " ", clean_text).strip()

        return clean_text
    except:
        # 如果BeautifulSoup解析失败，使用正则表达式
        text = str(text)
        # 移除HTML标签
        clean_text = re.sub(r"<[^>]+>", "", text)
        # 清理HTML实体
        clean_text = clean_text.replace("&nbsp;", " ")
        clean_text = clean_text.replace("&lt;", "<")
        clean_text = clean_text.replace("&gt;", ">")
        clean_text = clean_text.replace("&amp;", "&")
        clean_text = clean_text.replace("&quot;", '"')
        # 清理多余的空格
        clean_text = re.sub(r"\s+", " ", clean_text).strip()

        return clean_text


def clean_zhihu_mentions(text):
    """
    清除知乎特有的@用户名格式
    """
    if pd.isna(text):
        return ""

    # 匹配@开头的用户名
    pattern = r"@[^\s@]+\s*"
    cleaned_text = re.sub(pattern, "", text)

    # 清理多余的空格
    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()

    return cleaned_text


def remove_special_chars(text):
    """
    移除特殊字符和不可见字符
    """
    if pd.isna(text):
        return ""

    # 去除零宽字符
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]", "", text)

    # 去除异常的emoji组合
    text = re.sub(r"[\ufe00-\ufe0f]", "", text)  # 变体选择符
    text = re.sub(r"[\u200d]", "", text)  # 零宽连接符

    # 去除私用区字符
    text = re.sub(
        r"[\ue000-\uf8ff\U000f0000-\U000ffffd\U00100000-\U0010fffd]", "", text
    )

    return text


def clean_emoji(text):
    """
    清除文本中的表情符号
    """
    if pd.isna(text):
        return ""

    # 使用正则表达式匹配 [表情名] 格式的内容并删除
    cleaned_text = re.sub(r"\[.*?\]", "", text)

    # 清理多余的空格
    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()

    return cleaned_text


def clean_content_pipeline(text):
    """
    内容清理流水线
    """
    if pd.isna(text):
        return ""

    # 移除HTML标签
    text = remove_html_tags(text)

    # 清除@用户名
    text = clean_zhihu_mentions(text)

    # 移除特殊字符
    text = remove_special_chars(text)

    # 清除表情符号
    text = clean_emoji(text)

    # 最终清理空格
    text = re.sub(r"\s+", " ", text).strip()

    return text


def save_matched_info_from_meta_data(meta_file, review_file):
    # 读取CSV文件
    meta_df = pd.read_csv(
        meta_file,
        encoding="utf-8",
        dtype={
            "article_id": str,
            "question_id": str,
            "answer_id": str,
            "created_time": str,
        },
    )
    review_df = pd.read_csv(
        review_file,
        encoding="utf-8",
        dtype={
            "article_id": str,
            "question_id": str,
            "answer_id": str,
            "comment_id": str,
            "super_comment_id": str,
        },
    )

    # 检查是否包含 'article_id' 列
    if "article_id" not in meta_df.columns or "article_id" not in review_df.columns:
        print("CSV文件中不包含 'article_id' 列")
        return
    # 检查是否包含 'question_id' 和 'answer_id' 列
    if "question_id" not in meta_df.columns or "answer_id" not in meta_df.columns:
        print("CSV文件中不包含 'question_id' 或 'answer_id' 列")
        return
    # 检查是否包含 'comment_id' 和 'super_comment_id' 列
    if (
        "comment_id" not in review_df.columns
        or "super_comment_id" not in review_df.columns
    ):
        print("review_data.csv文件中不包含 'comment_id' 或 'super_comment_id' 列")
        return

    # 获取review_df中所有的article_id
    review_article_ids = set(review_df["article_id"].dropna().astype(str))
    # 获取review_df中所有的question_id和answer_id
    review_question_ids = set(review_df["question_id"].dropna().astype(str))
    review_answer_ids = set(review_df["answer_id"].dropna().astype(str))

    # 匹配到的行
    matched_meta = meta_df[
        meta_df["article_id"].astype(str).isin(review_article_ids)
        | meta_df["question_id"].astype(str).isin(review_question_ids)
        | meta_df["answer_id"].astype(str).isin(review_answer_ids)
    ]

    # 保存匹配到的行到新的CSV文件
    print(f"匹配到的元数据行数：{len(matched_meta)}")
    matched_meta_file = meta_file.replace(".csv", "_matched.csv")
    matched_meta.to_csv(matched_meta_file, index=False, encoding="utf-8")
    print(f"匹配到的元数据已保存到：{matched_meta_file}")


def save_matched_info_from_review_data(review_file, meta_file):
    # 读取CSV文件
    review_df = pd.read_csv(
        review_file,
        encoding="utf-8",
        dtype={
            "article_id": str,
            "question_id": str,
            "answer_id": str,
            "comment_id": str,
            "super_comment_id": str,
        },
    )
    meta_df = pd.read_csv(
        meta_file,
        encoding="utf-8",
        dtype={
            "article_id": str,
            "question_id": str,
            "answer_id": str,
            "created_time": str,
        },
    )

    # 检查是否包含 'article_id' 列
    if "article_id" not in review_df.columns or "article_id" not in meta_df.columns:
        print("CSV文件中不包含 'article_id' 列")
        return
    # 检查是否包含 'question_id' 和 'answer_id' 列
    if "question_id" not in review_df.columns or "answer_id" not in review_df.columns:
        print("CSV文件中不包含 'question_id' 或 'answer_id' 列")
        return
    # 检查是否包含 'comment_id' 和 'super_comment_id' 列
    if (
        "comment_id" not in review_df.columns
        or "super_comment_id" not in review_df.columns
    ):
        print("review_data.csv文件中不包含 'comment_id' 或 'super_comment_id' 列")
        return

    # 获取meta_df中所有的article_id
    meta_article_ids = set(meta_df["article_id"].dropna().astype(str))
    meta_question_ids = set(meta_df["question_id"].dropna().astype(str))
    meta_answer_ids = set(meta_df["answer_id"].dropna().astype(str))

    # 匹配到的行
    matched_review = review_df[
        review_df["article_id"].astype(str).isin(meta_article_ids)
        | (
            review_df["question_id"].astype(str).isin(meta_question_ids)
            & review_df["answer_id"].astype(str).isin(meta_answer_ids)
        )
    ]
    # 保存匹配到的行到新的CSV文件
    print(f"匹配到的评论数据行数：{len(matched_review)}")
    matched_review_file = review_file.replace(".csv", "_matched.csv")
    matched_review.to_csv(matched_review_file, index=False, encoding="utf-8")
    print(f"匹配到的评论数据已保存到：{matched_review_file}")


def clean_zhihu_comments_data(input_file, output_file):
    """
    清理知乎评论数据
    """
    # 读取CSV文件
    try:
        df = pd.read_csv(
            input_file,
            encoding="utf-8-sig",
            dtype={
                "article_id": str,
                "question_id": str,
                "answer_id": str,
                "comment_id": str,
                "super_comment_id": str,
            },
        )
    except:
        try:
            df = pd.read_csv(
                input_file,
                encoding="gbk",
                dtype={
                    "article_id": str,
                    "question_id": str,
                    "answer_id": str,
                    "comment_id": str,
                    "super_comment_id": str,
                },
            )
        except:
            df = pd.read_csv(
                input_file,
                encoding="utf-8",
                dtype={
                    "article_id": str,
                    "question_id": str,
                    "answer_id": str,
                    "comment_id": str,
                    "super_comment_id": str,
                },
            )

    # 输出原始条目数
    print(f"原始CSV文件包含 {len(df)} 条记录")

    # 检查是否存在content列
    if "content" not in df.columns:
        print("错误：CSV文件中没有找到'content'列")
        print(f"现有列名：{df.columns.tolist()}")
        exit()

    # 提取图片URL并添加到新列
    print("正在提取图片URL...")
    df["img_url"] = df["content"].apply(extract_image_urls)

    # 清理content列的HTML内容
    print("正在清理HTML内容...")
    df["content"] = df["content"].apply(clean_content_pipeline)

    # 过滤掉content内容长度小于3的记录
    print("正在过滤短内容...")
    # 首先处理NaN值
    df["content"] = df["content"].fillna("")

    # 修改过滤逻辑：保留文字长度>=3的记录 OR 包含图片的记录
    original_count = len(df)
    df = df[(df["content"].str.len() >= 7) | (df["img_url"] != "")]
    filtered_count = len(df)

    print(f"过滤后CSV文件包含 {filtered_count} 条记录")
    print(f"删除了 {original_count - filtered_count} 条短内容且无图片的记录")

    # 统计不同类型的记录数
    text_only = len(df[(df["content"].str.len() >= 7) & (df["img_url"] == "")])
    img_only = len(df[(df["content"].str.len() < 7) & (df["img_url"] != "")])
    text_and_img = len(
        df[(df["content"].str.len() >= 7) & (df["img_url"] != "")])
    img_count = len(df[df["img_url"] != ""])

    print(f"记录类型统计：")
    print(f"- 纯文字记录：{text_only}")
    print(f"- 纯图片记录：{img_only}")
    print(f"- 文字+图片记录：{text_and_img}")
    print(f"- 总图片记录数：{img_count}")
    # 保存清理后的数据
    df.to_csv(output_file, index=False, encoding="utf-8-sig")


def clean_zhihu_meta_data(input_file, output_file):
    # 读取CSV文件
    try:
        df = pd.read_csv(
            input_file,
            encoding="utf-8-sig",
            dtype={
                "article_id": str,
                "question_id": str,
                "answer_id": str,
                "comment_id": str,
                "super_comment_id": str,
            },
        )
    except:
        try:
            df = pd.read_csv(
                input_file,
                encoding="gbk",
                dtype={
                    "article_id": str,
                    "question_id": str,
                    "answer_id": str,
                    "comment_id": str,
                    "super_comment_id": str,
                },
            )
        except:
            df = pd.read_csv(
                input_file,
                encoding="utf-8",
                dtype={
                    "article_id": str,
                    "question_id": str,
                    "answer_id": str,
                    "comment_id": str,
                    "super_comment_id": str,
                },
            )

    # 输出原始条目数
    print(f"原始CSV文件包含 {len(df)} 条记录")

    # 检查是否存在content列
    if "content" not in df.columns:
        print("错误：CSV文件中没有找到'content'列")
        print(f"现有列名：{df.columns.tolist()}")
        exit()

    # 清理content列的HTML内容
    print("正在清理HTML内容...")
    df["content"] = df["content"].apply(clean_content_pipeline)
    df["title"] = df["title"].apply(clean_content_pipeline)

    # 清理content和title中的网址
    print("正在清理网址...")
    url_pattern = r"https?://[^\s]+"
    df["content"] = df["content"].str.replace(url_pattern, "", regex=True)
    df["title"] = df["title"].str.replace(url_pattern, "", regex=True)

    # 保存清理后的数据
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"清理后的数据已保存到：{output_file}")


if __name__ == "__main__":

    # 清理知乎评论数据
    input_file = (
        "zhihu/zhihu_results_final/comments_full_2.csv"  # 请修改为你的实际输入路径
    )
    output_file = "zhihu/zhihu_results_final/cleaned_zhihu_comments_data.csv"  # 请修改为你的实际输出路径
    clean_zhihu_comments_data(input_file, output_file)

    # 清理知乎元数据
    input_file = (
        "zhihu/zhihu_results_final/zhihu_meta_data.csv"  # 请修改为你的实际输入路径
    )
    output_file = "zhihu/zhihu_results_final/cleaned_zhihu_meta_data.csv"  # 请修改为你的实际输出路径
    clean_zhihu_meta_data(input_file, output_file)

    # 保存匹配信息
    save_matched_info_from_meta_data(
        "zhihu/zhihu_results_final/cleaned_zhihu_meta_data.csv",
        "zhihu/zhihu_results_final/cleaned_zhihu_comments_data.csv",
    )
    save_matched_info_from_review_data(
        "zhihu/zhihu_results_final/cleaned_zhihu_comments_data.csv",
        "zhihu/zhihu_results_final/cleaned_zhihu_meta_data_matched.csv",
    )
