import pandas as pd
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


def remove_html_tags(text):
    """
    移除HTML标签，保留纯文本内容
    """
    if pd.isna(text):
        return ""

    try:
        # 如果BeautifulSoup解析失败，使用正则表达式
        text = str(text)
        # 移除HTML标签
        clean_text = re.sub(r'<[^>]+>', '', text)
        # 清理HTML实体
        clean_text = clean_text.replace('&nbsp;', ' ')
        clean_text = clean_text.replace('&lt;', '<')
        clean_text = clean_text.replace('&gt;', '>')
        clean_text = clean_text.replace('&amp;', '&')
        clean_text = clean_text.replace('&quot;', '"')
        # 清理多余的空格
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    except Exception as e:
        print(f"Error removing HTML tags: {e}")
        clean_text = str(text)
        return clean_text


def clean_reply_text(text):
    """清理微博文本，去掉回复前缀、多余空格等"""
    if pd.isna(text):
        return ""

    # 去掉"回复@用户名:"的前缀
    text = re.sub(r'^回复@[^：:]+[：:]', '', text)

    # 去掉多余的空格和换行符
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def clean_weibo_mentions(text):
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


def clean_content_pipeline(text):
    """
    内容清理流水线
    """
    if pd.isna(text):
        return ""

    # 移除url
    text = remove_urls_from_text(text)

    # 清除@用户名
    text = clean_weibo_mentions(text)

    # 移除特殊字符
    text = remove_special_chars(text)

    # 清除表情符号
    text = clean_emoji(text)

    # 最终清理空格
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def save_matched_info_from_meta_data(meta_file, review_file):
    """
    根据评论数据中的mid，筛选出元数据中匹配的记录
    """
    # 读取CSV文件
    meta_df = pd.read_csv(meta_file, encoding='utf-8', dtype={'mid': str})
    review_df = pd.read_csv(review_file, encoding='utf-8', dtype={'mid': str, 'review_id': str, 'sup_comment': str})

    # 检查是否包含 'mid' 列
    if 'mid' not in meta_df.columns or 'mid' not in review_df.columns:
        print("CSV文件中不包含 'mid' 列")
        return

    # 获取review_df中所有的mid
    review_mids = set(review_df['mid'].dropna().astype(str))

    # 匹配到的行
    matched_meta = meta_df[meta_df['mid'].astype(str).isin(review_mids)]

    # 保存匹配到的行到新的CSV文件
    matched_meta_file = meta_file.replace('.csv', '_matched.csv')
    matched_meta.to_csv(matched_meta_file, index=False, encoding='utf-8')

    print(f"从 {len(meta_df)} 条元数据中匹配到 {len(matched_meta)} 条记录")
    print(f"匹配结果已保存到：{matched_meta_file}")


def save_matched_info_from_review_data(review_file, meta_file):
    """
    根据元数据中的mid，筛选出评论数据中匹配的记录
    """
    # 读取CSV文件
    review_df = pd.read_csv(review_file, encoding='utf-8', dtype={'mid': str, 'review_id': str, 'sup_comment': str})
    meta_df = pd.read_csv(meta_file, encoding='utf-8', dtype={'mid': str})

    # 检查是否包含 'mid' 列
    if 'mid' not in review_df.columns or 'mid' not in meta_df.columns:
        print("CSV文件中不包含 'mid' 列")
        return

    # 获取meta_df中所有的mid
    meta_mids = set(meta_df['mid'].dropna().astype(str))

    # 匹配到的行
    matched_review = review_df[review_df['mid'].astype(str).isin(meta_mids)]

    # 保存匹配到的行到新的CSV文件
    matched_review_file = review_file.replace('.csv', '_matched.csv')
    matched_review.to_csv(matched_review_file, index=False, encoding='utf-8')

    print(f"从 {len(review_df)} 条评论数据中匹配到 {len(matched_review)} 条记录")
    print(f"匹配结果已保存到：{matched_review_file}")


def clean_weibo_comments_data(input_file, output_file):
    """
    清理微博评论数据
    """
    # 读取CSV文件
    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig", dtype={'mid': str, 'review_id': str, 'sup_comment': str, 'uid': str})
    except:
        try:
            df = pd.read_csv(input_file, encoding="gbk", dtype={'mid': str, 'review_id': str, 'sup_comment': str, 'uid': str})
        except:
            df = pd.read_csv(input_file, encoding="utf-8", dtype={'mid': str, 'review_id': str, 'sup_comment': str, 'uid': str})

    # 输出原始条目数
    print(f"原始CSV文件包含 {len(df)} 条记录")

    # 检查是否存在text_raw列
    if 'text_raw' not in df.columns:
        print("错误：CSV文件中没有找到'text_raw'列")
        print(f"现有列名：{df.columns.tolist()}")
        return

    # 如果img_url列不存在，从text_raw中提取图片URL
    if 'img_url' not in df.columns:
        print("正在提取图片URL...")
        df['img_url'] = df['text_raw'].apply(extract_image_urls)
    else:
        print("img_url列已存在，跳过图片URL提取")

    # 清理text_raw列的内容
    print("正在清理内容...")
    df['text_raw'] = df['text_raw'].apply(clean_reply_text)
    df['text_raw'] = df['text_raw'].apply(clean_content_pipeline)

    # 过滤掉text_raw内容长度小于3的记录
    print("正在过滤短内容...")
    # 首先处理NaN值
    df['text_raw'] = df['text_raw'].fillna('')

    # 修改过滤逻辑：保留文字长度>=3的记录 OR 包含图片的记录
    original_count = len(df)
    df = df[(df['text_raw'].str.len() >= 7) | (df['img_url'] != '')]
    filtered_count = len(df)

    print(f"过滤后CSV文件包含 {filtered_count} 条记录")
    print(f"删除了 {original_count - filtered_count} 条短内容且无图片的记录")

    # 统计不同类型的记录数
    text_only = len(df[(df['text_raw'].str.len() >= 7) & (df['img_url'] == '')])
    img_only = len(df[(df['text_raw'].str.len() < 3) & (df['img_url'] != '')])
    text_and_img = len(df[(df['text_raw'].str.len() >= 7) & (df['img_url'] != '')])
    img_count = len(df[df['img_url'] != ''])

    print(f"记录类型统计：")
    print(f"- 纯文字记录：{text_only}")
    print(f"- 纯图片记录：{img_only}")
    print(f"- 文字+图片记录：{text_and_img}")
    print(f"- 总图片记录数：{img_count}")

    # 保存清理后的数据
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"清理后的数据已保存到：{output_file}")


def clean_weibo_meta_data(input_file, output_file):
    """
    清理微博元数据
    """
    # 读取CSV文件
    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig", dtype={'mid': str, 'uid': str})
    except:
        try:
            df = pd.read_csv(input_file, encoding="gbk", dtype={'mid': str, 'uid': str})
        except:
            df = pd.read_csv(input_file, encoding="utf-8", dtype={'mid': str, 'uid': str})

    # 输出原始条目数
    print(f"原始CSV文件包含 {len(df)} 条记录")

    # 检查需要清理的列
    columns_to_clean = []
    if 'content' in df.columns:
        columns_to_clean.append('content')
    if 'text' in df.columns:
        columns_to_clean.append('text')
    if 'title' in df.columns:
        columns_to_clean.append('title')

    # #  把时间列从datatime转换为timestamp字符串
    # if 'created_at' in df.columns:
    #     df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce').astype('int64') // 10**9
    #     df['created_at'] = df['created_at'].astype(str)

    if not columns_to_clean:
        print("错误：CSV文件中没有找到需要清理的文本列（content/text/title）")
        print(f"现有列名：{df.columns.tolist()}")
        return

    # 清理文本列的HTML内容
    print(f"正在清理HTML内容，涉及列：{columns_to_clean}")
    for col in columns_to_clean:
        df[col] = df[col].apply(clean_content_pipeline)

    # 清理文本列中的网址
    print("正在清理网址...")
    url_pattern = r'https?://[^\s]+'
    for col in columns_to_clean:
        df[col] = df[col].str.replace(url_pattern, '', regex=True)

    # 保存清理后的数据
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"清理后的数据已保存到：{output_file}")


def analyze_weibo_data_structure(file_path):
    """
    分析微博数据结构
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


# 去除一个csv文件的qwen,qwen_sentiment,qwen_intent三列
def remove_qwen_columns(input_file, output_file):
    """
    去除指定CSV文件中的qwen, qwen_sentiment, qwen_intent三列
    """
    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig", dtype=str)
    except:
        try:
            df = pd.read_csv(input_file, encoding="gbk", dtype=str)
        except:
            df = pd.read_csv(input_file, encoding="utf-8", dtype=str)

    # 检查并删除指定列
    columns_to_remove = ['qwen', 'qwen_sentiment', 'qwen_intent']
    for col in columns_to_remove:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    # 保存清理后的数据
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"已将 {input_file} 中的指定列删除，并保存到 {output_file}")


def remove_multi_stance(input_file, output_file):
    """
    有些评论因为内容是分点的，所以会有多个立场，情感，意图
    但是这些内容因为处理不得当，全部被保留了下来且只放在了stance列
    但是因为我们只需要一个立场，情感，意图，所以需要去除多余的立场，情感，意图，并把对应的情感和意图放在对应的列中
    """
    # 读取CSV文件
    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig", dtype=str)
    except:
        try:
            df = pd.read_csv(input_file, encoding="gbk", dtype=str)
        except:
            df = pd.read_csv(input_file, encoding="utf-8", dtype=str)

    # 检查是否包含 'stance' 列
    if 'stance' not in df.columns:
        print("错误：CSV文件中没有找到'stance'列")
        return

    print(f"原始数据包含 {len(df)} 条记录")
    
    # 如果不存在 sentiment 和 intent 列，则创建它们
    if 'sentiment' not in df.columns:
        df['sentiment'] = ''
    if 'intent' not in df.columns:
        df['intent'] = ''
    
    # 应用解析函数
    print("正在解析多立场数据...")
    
    # 直接遍历DataFrame，逐行处理
    processed_count = 0
    for idx, row in df.iterrows():
        stance_str = row['stance']
        
        # 检查是否需要处理
        if pd.isna(stance_str) or stance_str == '':
            continue
            
        stance_str = str(stance_str).strip()
        bracket_count = stance_str.count('[')
        
        if bracket_count < 3:
            continue  # 不是多立场格式，跳过
        
        try:
            # 使用正则表达式提取所有的 [xxx, xxx] 格式内容
            pattern = r'\[([^\]]+)\]'
            matches = re.findall(pattern, stance_str)
            
            if len(matches) >= 3:
                # 解析第一组（立场）
                stance_parts = matches[0].split(',')
                stance = stance_parts[0].strip() if len(stance_parts) > 0 else ''
                stance_desc = stance_parts[1].strip() if len(stance_parts) > 1 else ''
                
                # 解析第二组（情感）  
                sentiment_parts = matches[1].split(',')
                sentiment = sentiment_parts[0].strip() if len(sentiment_parts) > 0 else ''
                sentiment_desc = sentiment_parts[1].strip() if len(sentiment_parts) > 1 else ''
                
                # 解析第三组（意图）
                intent_parts = matches[2].split(',')
                intent = intent_parts[0].strip() if len(intent_parts) > 0 else ''
                intent_desc = intent_parts[1].strip() if len(intent_parts) > 1 else ''
                
                # 直接更新原DataFrame的对应行
                df.at[idx, 'stance'] = f'[{stance}, {stance_desc}]' if stance_desc else f'[{stance}]'
                df.at[idx, 'sentiment'] = f'[{sentiment}, {sentiment_desc}]' if sentiment_desc else f'[{sentiment}]'
                df.at[idx, 'intent'] = f'[{intent}, {intent_desc}]' if intent_desc else f'[{intent}]'
                
                processed_count += 1
                
        except Exception as e:
            print(f"解析错误 {stance_str}: {e}")
            continue
    
    print(f"发现并处理了 {processed_count} 行包含多立场信息")
    
    # 统计处理结果
    valid_stance = (df['stance'] != '').sum()
    valid_sentiment = (df['sentiment'] != '').sum()
    valid_intent = (df['intent'] != '').sum()
    
    print(f"处理结果统计:")
    print(f"- 总记录数: {len(df)}")
    print(f"- 处理的多立场记录数: {processed_count}")
    print(f"- 有效立场数: {valid_stance}")
    print(f"- 有效情感数: {valid_sentiment}")
    print(f"- 有效意图数: {valid_intent}")
    
    # 显示一些处理后的示例
    if processed_count > 0:
        print("\n处理后的示例数据:")
        # 找到一些被处理过的行来展示
        sample_count = 0
        for idx, row in df.iterrows():
            if sample_count >= 3:
                break
            if '[' in str(row['stance']) and '[' in str(row['sentiment']) and '[' in str(row['intent']):
                print(f"立场: {row['stance']}")
                print(f"情感: {row['sentiment']}")  
                print(f"意图: {row['intent']}")
                print("-" * 30)
                sample_count += 1
    
    # 保存处理后的数据
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"处理后的数据已保存到: {output_file}")
    
    return df

    


if __name__ == "__main__":

    remove_multi_stance(
        input_file='weibo/weibo_final_results/cleaned_weibo_comments_ds_renamed.csv',
        output_file='weibo/weibo_final_results/cleaned_weibo_comments_ds_split.csv')

    # 分析数据结构（可选）
    # print("分析数据结构...")
    # # analyze_weibo_data_structure("meta_data_updated_filtered.csv")
    # # analyze_weibo_data_structure("cleaned_review_data.csv")

    # # 清理微博评论数据
    # print("\n开始清理微博评论数据...")
    # input_file = "weibo/weibo_final_results/comments_ds.csv"  # 请修改为你的实际输入路径
    # output_file = "weibo/weibo_final_results/cleaned_weibo_comments_ds.csv"  # 请修改为你的实际输出路径
    # clean_weibo_comments_data(input_file, output_file)

    # # 清理微博元数据
    # print("\n开始清理微博元数据...")
    # input_file = "weibo/weibo_final_results/cleaned_weibo_meta_data_matched_stance_sentiment_intent.csv"  # 请修改为你的实际输入路径
    # output_file = "weibo/weibo_final_results/cleaned_weibo_meta_data.csv"  # 请修改为你的实际输出路径
    # clean_weibo_meta_data(input_file, output_file)

    # # 保存匹配信息
    # print("\n开始保存匹配信息...")
    # save_matched_info_from_meta_data('weibo/weibo_final_results/cleaned_weibo_meta_data.csv',
    #                                  'weibo/weibo_final_results/cleaned_weibo_comments_ds.csv')
    # save_matched_info_from_review_data('weibo/weibo_final_results/cleaned_weibo_comments_ds.csv',
    #                                    'weibo/weibo_final_results/cleaned_weibo_meta_data_matched.csv')

    # remove_qwen_columns(
    #     input_file='weibo/weibo_final_results/comments_qwen_ds.csv',
    #     output_file='weibo/weibo_final_results/comments_ds.csv'
    # )

    # input_file = 'weibo/weibo_final_results/cleaned_weibo_comments_ds_matched.csv'

    # df = pd.read_csv(input_file, encoding="utf-8-sig", dtype=str)
    # df.rename(columns={'mid': 'article_id',
    #                    'review_id': 'comment_id',
    #                    'sup_comment': 'parent_comment_id',
    #                    'created_at': 'created_time',
    #                    'text_raw': 'content',
    #                    'source': 'location',
    #                    'like': 'like_count',
    #                    'img_url': 'img_urls',
    #                    'review_num': 'comment_count',
    #                    'ds_stance': 'stance',
    #                    'ds_sentiment': 'sentiment',
    #                    'ds_intent': 'intent'}, inplace=True)

    # output_file = 'weibo/weibo_final_results/cleaned_weibo_comments_ds_renamed.csv'
    # df.to_csv(output_file, index=False, encoding="utf-8-sig")

    # input_file = 'weibo/weibo_final_results/cleaned_weibo_meta_data_matched.csv'
    # df_meta = pd.read_csv(input_file, encoding="utf-8-sig", dtype=str)
    # df_meta.rename(columns={'mid': 'article_id',
    #                         'created_at': 'created_time',
    #                         'text': 'content',
    #                         'region': 'location',
    #                         'pic_num': 'img_count',
    #                         'pic_url': 'img_urls',
    #                         'video_url': 'video_urls',
    #                         'comments_count': 'comment_count',
    #                         'multimodal_stance': 'stance',
    #                         'multimodal_sentiment': 'sentiment',
    #                         'multimodal_intent': 'intent'}, inplace=True)

    # output_file_meta = 'weibo/weibo_final_results/cleaned_weibo_meta_data_renamed.csv'
    # df_meta.to_csv(output_file_meta, index=False, encoding="utf-8-sig")
