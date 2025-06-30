import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import re

matplotlib.rc('font', family='SimHei')  # 设置字体为黑体，支持中文显示


# 从csv文件中随机选取100条数据，保存在output_file中
def sample_random_data(input_file, output_file, sample_size=100):
    """
    从CSV文件中随机选取指定数量的数据，并保存到新的CSV文件中。

    :param input_file: 输入CSV文件路径
    :param output_file: 输出CSV文件路径
    :param sample_size: 随机选取的数据条数，默认为100
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(input_file, encoding='utf-8-sig', low_memory=False, dtype=str)
        # 随机选取指定数量的数据
        sampled_df = df.sample(n=sample_size, random_state=42)
        # 保存到新的CSV文件
        sampled_df.to_csv(output_file, index=False)
        print(f"已从 {input_file} 中随机选取 {sample_size} 条数据，并保存到 {output_file}")
    except Exception as e:
        print(f"处理数据时出错: {e}")


# 从csv文件中读取时间戳，并按照时间戳排序和统计不同天数，只关注时间戳列
def read_and_sort_timestamps(input_file, timestamp_col='timestamp'):
    """
    从CSV文件中读取时间戳列，按照时间戳排序，并统计不同天数。

    :param input_file: 输入CSV文件路径
    :param timestamp_col: 时间戳列名，默认为'timestamp'
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(input_file)
        # 确保时间戳列存在
        if timestamp_col not in df.columns:
            raise ValueError(f"列 '{timestamp_col}' 在输入文件中不存在。")

        # 时间戳为字符串, 转换为时间格式
        df[timestamp_col] = pd.to_datetime(df[timestamp_col].astype(float), unit='s')
        # 按照时间戳排序
        df = df.sort_values(by=timestamp_col)
        # 提取日期部分
        df['date'] = df[timestamp_col].dt.date
        # 统计不同日期的数量
        date_counts = df['date'].value_counts().sort_index()
        # 打印统计结果
        # print("不同日期的数量统计:")
        # for date, count in date_counts.items():
        #     print(f"{date}: {count} 条数据")

        # 显示统计结果的图表，每个柱状图上方显示具体的数量
        plt.figure(figsize=(12, 6))
        date_counts.plot(kind='bar', color='skyblue')
        plt.title('不同日期的数据条数统计')
        plt.xlabel('日期')
        plt.ylabel('数据条数')
        plt.xticks(rotation=45)
        plt.tight_layout()
        for i, count in enumerate(date_counts):
            plt.text(i, count + 0.5, str(count), ha='center', va='bottom')
        save_path = input_file.replace('.csv', '_date_counts_statistics.png')
        plt.savefig(save_path)
        print(f"统计结果已保存为 '{save_path}'")

    except Exception as e:
        print(f"处理时间戳时出错: {e}")


# 把一个csv文件中的时间戳转换成另一个csv文件的时间戳(假设两个csv文件其余内容相同,只是一个csv文件时间戳有问题,需要修正)
def convert_timestamps(input_file, output_file, timestamp_col='timestamp'):
    """
    从一个CSV文件中读取时间戳，并将其转换为另一个CSV文件的时间戳。

    :param input_file: 输入CSV文件路径
    :param output_file: 输出CSV文件路径
    :param timestamp_col: 时间戳列名，默认为'timestamp'
    """
    try:
        # 1. 读取两个CSV文件
        # 2. 从第一个CSV文件中读取时间戳
        # 3. 将时间戳保存到第二个CSV文件中
        df_input = pd.read_csv(input_file)
        df_output = pd.read_csv(output_file)
        # 确保时间戳列存在
        if timestamp_col not in df_input.columns:
            raise ValueError(f"列 '{timestamp_col}' 在输入文件中不存在。")
        if timestamp_col not in df_output.columns:
            raise ValueError(f"列 '{timestamp_col}' 在输出文件中不存在。")

        # 读取时间戳列
        timestamps = df_input[timestamp_col]

        # 将第二个CSV文件的时间戳列和第一个CSV文件的时间戳列进行转换,保存字符串
        df_output[timestamp_col] = timestamps.astype(str)
        # 保存转换后的数据到新的CSV文件
        df_output.to_csv(output_file, index=False)
        print(f"已将时间戳从 {input_file} 转换并保存到 {output_file}")

    except Exception as e:
        print(f"处理时间戳转换时出错: {e}")


def split_multi_stance(input_file, output_file):
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

    # 示例：将CSV文件中的多立场数据解析并保存到新的CSV文件中
    split_multi_stance('weibo/weibo_final_results/cleaned_weibo_comments_ds_renamed.csv',
                       'weibo/weibo_final_results/cleaned_weibo_comments_ds_split.csv')
    split_multi_stance('douyin/douyin_results_final/cleaned_douyin_comments_ds_renamed.csv',
                       'douyin/douyin_results_final/cleaned_douyin_comments_ds_split.csv')
    split_multi_stance('zhihu/zhihu_results_final/cleaned_zhihu_comments_ds_renamed.csv',
                       'zhihu/zhihu_results_final/cleaned_zhihu_comments_ds_split.csv')
    split_multi_stance('xhs/xhs_results_final/cleaned_xhs_comments_ds_renamed.csv',
                       'xhs/xhs_results_final/cleaned_xhs_comments_ds_split.csv')
    split_multi_stance('weixin/weixin_final_results/weixin_comments_clean_with_ds_renamed.csv',
                       'weixin/weixin_final_results/cleaned_weixin_comments_ds_split.csv')

    # # # 输入CSV文件路径
    # input_csv_file = ['douyin/douyin_results_final/comments_ds.csv',
    #                   'weibo/weibo_final_results/comments_ds.csv',
    #                   'zhihu/zhihu_results_final/comments_ds.csv',
    #                   'xhs/xhs_results_final/comments_ds.csv',
    #                   'weixin/weixin_final_results/comments_ds.csv']

    # # 输出CSV文件路径
    # output_csv_file = ['sample_data/douyin_sample_100.csv',
    #                    'sample_data/weibo_sample_100.csv',
    #                    'sample_data/zhihu_sample_100.csv',
    #                    'sample_data/xhs_sample_100.csv',
    #                    'sample_data/weixin_sample_100.csv']

    # # 随机选取100条数据
    # for input_file, output_file in zip(input_csv_file, output_csv_file):
    #     sample_random_data(input_file, output_file, sample_size=100)

    # 输入CSV文件路径
    # input_csv_file = 'weibo/weibo_final_results/cleaned_weibo_comments_data_matched.csv'
    # input_csv_file = 'weibo/weibo_final_results/cleaned_weibo_meta_data_matched.csv'
    # input_csv_file = 'zhihu/zhihu_results_final/cleaned_zhihu_comments_data_0619_matched.csv'
    # input_csv_file = 'douyin/douyin_results_final/cleaned_douyin_comments_data_matched.csv'
    # input_csv_file = 'douyin/douyin_results_final/cleaned_douyin_meta_data_matched.csv'
    # input_csv_file = 'xhs/xhs_results_final/cleaned_xhs_comments_data_matched.csv'
    # input_csv_file = 'weixin/weixin_final_results/weixin_comments_clean.csv'
    # read_and_sort_timestamps(input_csv_file, timestamp_col='created_at')

    # convert_timestamps('weibo/weibo_final_results/cleaned_weibo_meta_data2_matched.csv',
    #                    'weibo/weibo_final_results/cleaned_weibo_meta_data_matched_stance_sentiment_intent.csv',
    #                    timestamp_col='created_at')
