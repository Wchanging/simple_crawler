import pandas as pd
import json
import os
import time

# merge_comments_data from json files to csv files


def merge_comments(input_dir, output_file):
    """
    合并评论数据 | Merge comment data from JSON files to a single CSV file.

    :param input_dir: 输入目录，包含多个JSON文件 | Input directory containing multiple JSON files.
    :param output_file: 输出CSV文件路径 | Output CSV file path.
    """
    all_comments = []

    # 遍历输入目录中的所有JSON文件 | Iterate through all JSON files in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(input_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for i in range(len(data)):
                    comment = data[i]
                    # 提取评论信息 | Extract comment information
                    comment_info = {
                        'cid': comment.get('cid', ''),
                        'text': comment.get('text', ''),
                        'aweme_id': comment.get('aweme_id', ''),
                        'create_time': comment.get('create_time', ''),
                        'digg_count': comment.get('digg_count', 0),
                        'status': comment.get('status', 0),
                        'uid': comment.get('uid', ''),
                        'nickname': comment.get('nickname', ''),
                        'reply_id': comment.get('reply_id', ''),
                        'reply_comment': comment.get('reply_comment', ''),
                        'text_extra': comment.get('text_extra', []),
                        'reply_to_reply_id': comment.get('reply_to_reply_id', ''),
                        'is_note_comment': comment.get('is_note_comment', 0),
                        'ip_label': comment.get('ip_label', ''),
                        'root_comment_id': comment.get('root_comment_id', ''),
                        'level': comment.get('level', 0),
                        'cotent_type': comment.get('cotent_type', 0),
                    }
                    all_comments.append(comment_info)

    # 将所有评论数据转换为DataFrame | Convert all comments to a DataFrame
    df = pd.DataFrame(all_comments)

    # 写下表头
    if not df.empty:
        df.columns = ['cid', 'text', 'aweme_id', 'create_time', 'digg_count', 'status', 'uid', 'nickname',
                      'reply_id', 'reply_comment', 'text_extra', 'reply_to_reply_id',
                      'is_note_comment', 'ip_label', 'root_comment_id', 'level', 'cotent_type']
    else:
        print("没有找到任何评论数据。")  # No comment data found
        return

    # 根据 cid, aweme_id, create_time 去重 | Remove duplicates based on cid, aweme_id, create_time
    df.drop_duplicates(subset=['cid', 'text', 'aweme_id', 'create_time'], inplace=True)
    # 把text小于等于3的评论删除 | Remove comments with text length less than or equal to 3
    df = df[df['text'].str.len() > 3]
    # 重置索引 | Reset index
    df.reset_index(drop=True, inplace=True)
    # 检查是否有数据 | Check if there is any data
    if df.empty:
        print("没有找到任何评论数据。")  # No comment data found
        return
    # 保存到CSV文件 | Save to CSV file
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"合并完成，数据已保存到 {output_file}")  # Merge complete, data saved to output_file


def merge_body(input_dir, output_file):
    """
    合并视频数据 | Merge video data from JSON files to a single CSV file.

    :param input_dir: 输入目录，包含多个JSON文件 | Input directory containing multiple JSON files.
    :param output_file: 输出CSV文件路径 | Output CSV file path.
    """
    all_videos = []

    # 遍历输入目录中的所有JSON文件 | Iterate through all JSON files in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(input_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    # 提取视频信息 | Extract video information
                    video_info = {
                        'aweme_id': item.get('aweme_id', ''),
                        'desc': item.get('desc', ''),
                        # 'create_time': item.get('create_time', '')
                        # time转成 timestamp, 获取到的是字符串 比如 '2023-10-01 12:00:00' | Convert time to timestamp, get a string like '2023-10-01 12:00:00'
                        'create_time': int(time.mktime(time.strptime(item.get('create_time', ''), '%Y-%m-%d %H:%M:%S'))),
                        'author_uid': item.get('author', ''),
                        'author_name': item.get('author_name', ''),
                        'gender': item.get('gender', 0),
                        'follower_count': item.get('follower_count', 0),
                        'music_id': item.get('music_id', ''),
                        'music_urls': item.get('music_urls', []),
                        'video_url': item.get('video_url', []),
                        'duration': item.get('duration', 0),
                        'cover_url': item.get('cover_url', []),
                        'share_url': item.get('share_url', ''),
                        'comment_count': item.get('comment_count', 0),
                        'digg_count': item.get('digg_count', 0),
                        'share_count': item.get('share_count', 0),
                        'collect_count': item.get('collect_count', 0),
                    }
                    all_videos.append(video_info)

    # 将所有视频数据转换为DataFrame | Convert all videos to a DataFrame
    df = pd.DataFrame(all_videos)
    # 写下表头
    if not df.empty:
        df.columns = ['aweme_id', 'desc', 'create_time', 'author_uid', 'author_name',
                      'gender', 'follower_count', 'music_id', 'music_urls',
                      'video_url', 'duration', 'cover_url', 'share_url',
                      'comment_count', 'digg_count', 'share_count', 'collect_count']
    else:
        print("没有找到任何视频数据。")
        return
    # 根据 aweme_id, desc, create_time 去重 | Remove duplicates based on aweme_id, desc, create_time
    df.drop_duplicates(subset=['aweme_id', 'desc', 'create_time'], inplace=True)
    # 重置索引 | Reset index
    df.reset_index(drop=True, inplace=True)
    # 检查是否有数据 | Check if there is any data
    if df.empty:
        print("没有找到任何视频数据。")  # No video data found
        return
    # 保存到CSV文件 | Save to CSV file
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"合并完成，数据已保存到 {output_file}")  # Merge complete, data saved to output_file


if __name__ == "__main__":
    input_directory = 'douyin/douyin_results_final/douyin_results_comments'  # 输入目录 | Input directory
    output_csv_file = 'douyin/douyin_results_final/merged_comments.csv'  # 输出CSV文件路径 | Output CSV file path

    merge_comments(input_directory, output_csv_file)  # 调用函数合并评论数据 | Call the function to merge comment data

    # input_directory = 'douyin/'  # 输入目录 | Input directory
    # output_csv_file = 'douyin/merged_body.csv'  # 输出CSV文件路径 | Output CSV file path
    # merge_body(input_directory, output_csv_file)  # 调用函数合并视频数据 | Call the function to merge video data
