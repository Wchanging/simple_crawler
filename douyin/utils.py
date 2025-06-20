import json
import os
import re
import time
from datetime import datetime


def get_json_data_from_file(file_path):
    """
    从指定的JSON文件中读取数据。

    :param file_path: JSON文件的路径
    """
    if not os.path.exists(file_path):
        print(f"文件 {file_path} 不存在")
        return None

    with open(file_path, 'r', encoding='utf-8') as file:
        try:
            data = json.load(file)
            return data
        except json.JSONDecodeError as e:
            print(f"读取JSON文件时出错: {e}")
            return None


def get_data_from_json(data):
    data_list = []
    data = data['data'].get('data', [])
    for item in data:
        if item.get('type') != 1:
            continue
        aweme_id = item.get('aweme_info', {}).get('aweme_id', '')
        desc = item.get('aweme_info', {}).get('desc', '')
        create_time = item.get('aweme_info', {}).get('create_time', [0])
        # author = item['aweme_info']['author']['uid'],
        author = item['aweme_info']['author'].get('uid', '')
        author_name = item['aweme_info']['author'].get('nickname', '')
        gender = item['aweme_info']['author'].get("gender", 0)
        try:
            # music_id = item['aweme_info']['music']['id_str'],
            music_id = item['aweme_info']['music'].get('id_str', '')
            music_urls = item['aweme_info']['music'].get('play_url', {}).get('url_list', [])
        except KeyError:
            music_id = ""
            music_urls = []
        video_url = item['aweme_info']['video'].get('play_addr', {}).get('url_list', [])
        duration = item['aweme_info']['video'].get('duration', 0)
        cover_url = item['aweme_info']['video'].get('cover', {}).get('url_list', [])
        share_url = item['aweme_info'].get('share_info', {}).get('share_url', '')
        # statistics 评论数 点赞数 分享数 收藏数
        # comment_count = item['aweme_info']['statistics']['comment_count'],
        comment_count = item['aweme_info']['statistics'].get('comment_count', 0)
        # digg_count = item['aweme_info']['statistics']['digg_count'],
        digg_count = item['aweme_info']['statistics'].get('digg_count', 0)
        # share_count = item['aweme_info']['statistics']['share_count'],
        share_count = item['aweme_info']['statistics'].get('share_count', 0)
        # collect_count = item['aweme_info']['statistics']['collect_count'],
        collect_count = item['aweme_info']['statistics'].get('collect_count', 0)
        # author_stats 粉丝数 关注数 作品数
        # follower_count = item['aweme_info']['author']['follower_count'],
        follower_count = item['aweme_info']['author'].get('follower_count', 0)

        data_list.append({
            'aweme_id': aweme_id,
            'desc': desc,
            'create_time': datetime.fromtimestamp(create_time).strftime('%Y-%m-%d %H:%M:%S'),
            'author': author,
            'author_name': author_name,
            'gender': gender,
            'follower_count': follower_count,
            'music_id': music_id,
            'music_urls': music_urls,
            'video_url': video_url,
            'duration': duration,
            'cover_url': cover_url,
            'share_url': share_url,
            'comment_count': comment_count,
            'digg_count': digg_count,
            'share_count': share_count,
            'collect_count': collect_count
        })

    return data_list


def save_data_to_json(data, file_path):
    """
    将数据保存到指定的JSON文件中。

    :param data: 要保存的数据
    """
    with open(file_path, 'w', encoding='utf-8') as file:
        try:
            json.dump(data, file, ensure_ascii=False, indent=4)
            print(f"数据已保存到 {file_path}")
        except Exception as e:
            print(f"保存数据时出错: {e}")


def delete_same_data(data):
    """
    删除重复的数据。

    :param data: 包含数据的列表
    :return: 删除重复数据后的列表
    """
    seen = set()
    unique_data = []
    for item in data:
        aweme_id = item.get('aweme_id')
        if aweme_id not in seen:
            seen.add(aweme_id)
            unique_data.append(item)
    return unique_data


if __name__ == "__main__":
    # 打开指定文件夹，读取所有JSON文件
    folder_path = 'douyin/douyin_results_tikhub_new'
    all_data = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.json'):
            file_path = os.path.join(folder_path, file_name)
            data = get_json_data_from_file(file_path)
            if data:
                all_data.extend(get_data_from_json(data))
    # 删除重复数据
    all_data = delete_same_data(all_data)
    # 打印数据条数
    print(f"总数据条数: {len(all_data)}")
    # 保存所有数据到一个新的JSON文件
    output_file_path = 'douyin/all_data2.json'
    save_data_to_json(all_data, output_file_path)

    # # 对比两个JSON文件的数据，将不同的数据保存到一个新的JSON文件
    # file1_path = 'douyin/all_data.json'
    # file2_path = 'douyin/all_data2.json'
    # file1_data = get_json_data_from_file(file1_path)
    # file2_data = get_json_data_from_file(file2_path)
    # if file1_data and file2_data:
    #     file1_set = {item['aweme_id'] for item in file1_data}
    #     file2_set = {item['aweme_id'] for item in file2_data}

    #     # 找出在file2中但不在file1中的数据
    #     diff_data = [item for item in file2_data if item['aweme_id'] not in file1_set]

    #     # 打印不同数据的条数
    #     print(f"不同数据条数: {len(diff_data)}")

    #     # 保存不同的数据到新的JSON文件
    #     diff_file_path = 'douyin/diff_data.json'
    #     save_data_to_json(diff_data, diff_file_path)
    # else:
    #     print("无法读取文件数据，检查文件路径和内容格式。")
