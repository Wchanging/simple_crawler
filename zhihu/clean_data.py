import pandas as pd
import os
import json
import re

# zhihu\zhihu_data.csv里面的数据内容content部分因为是直接抓的，全是html标签，导致数据不干净
# # 需要清洗一下，去掉html标签，保留纯文本内容


def clean_html_tags(content):
    # 使用正则表达式去除HTML标签
    clean = re.compile('<.*?>')
    return re.sub(clean, '', content)


if __name__ == "__main__":
    # 读取csv文件
    csv_file = 'zhihu/zhihu_data.csv'
    # 读取第一行数据，尝试函数，打印看看
    if not os.path.exists(csv_file):
        print(f"文件 {csv_file} 不存在")
    else:
        df = pd.read_csv(csv_file, encoding="utf-8-sig", dtype={'article_id': str, 'answer_id': str, 'question_id': str, 'comment_id': str})
        # 看看多少行数据
        print(f"读取到 {len(df)} 行数据")
        # if 'content' in df.columns:
        #     # 清洗content列
        #     df['content'] = df['content'].apply(clean_html_tags)
        #     # 保存清洗后的数据到新的csv文件
        #     cleaned_csv_file = 'zhihu/zhihu_data_cleaned.csv'
        #     df.to_csv(cleaned_csv_file, index=False, encoding='utf-8-sig')
        #     print(f"清洗后的数据已保存到 {cleaned_csv_file}")
        # else:
        #     print("CSV文件中没有'content'列")
