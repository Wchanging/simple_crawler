# 导入异步io库 | Import asyncio
import asyncio
import json
import os
import requests
import time
from dotenv import load_dotenv


# 从xlsx中读取每个url，存到txt文件中
def read_urls_from_xlsx(file_path, save_path):
    """
    从指定的xlsx文件中读取URL列表，并将其保存到txt文件中。

    :param file_path: xlsx文件的路径
    :param save_path: 保存URL的txt文件路径
    """
    import pandas as pd

    if not os.path.exists(file_path):
        print(f"文件 {file_path} 不存在")
        return

    # 读取xlsx文件，注意没有表头
    try:
        df = pd.read_excel(file_path, header=None)
        urls = df[0].tolist()  # 假设URL在第一列
    except Exception as e:
        print(f"读取xlsx文件时出错: {e}")
        return
    # 保存到txt文件
    with open(save_path, 'w', encoding='utf-8') as file:
        for url in urls:
            if isinstance(url, str) and url.strip():  # 确保是字符串且不为空
                file.write(url.strip() + '\n')


# 从txt文件中读取每个url
def read_urls_from_txt(file_path):
    """
    从指定的txt文件中读取URL列表。

    :param file_path: txt文件的路径
    :return: URL列表
    """
    if not os.path.exists(file_path):
        print(f"文件 {file_path} 不存在")
        return []

    with open(file_path, 'r', encoding='utf-8') as file:
        urls = [line.strip() for line in file if line.strip()]
    return urls


def save_data_to_json(urls):
    """
    将数据保存到指定的JSON文件中。

    :param urls: 要保存的数据的url列表
    """
    for i, url in enumerate(urls):
        api_url = "https://api.tikhub.io/api/v1/wechat_mp/web/fetch_mp_article_detail_json?url=" + url
        headers = {
            'Authorization': 'Bearer ' + api_key,
        }

        max_retries = 5  # 最大重试次数
        retry_count = 0

        while retry_count < max_retries:
            try:
                response = requests.get(api_url, headers=headers)
                response.raise_for_status()  # 检查请求是否成功

                # 解析JSON响应
                data = response.json()

                # 检查data是否为null
                if data.get('data') is None:
                    retry_count += 1
                    print(f"第{i}个URL - 第{retry_count}次尝试: data为null, 等待10秒后重试...")
                    time.sleep(10)  # 等待10秒后重试
                    continue

                # data不为null，保存数据
                with open(f'weixin/weixin_articles/weixin_article_data_{i}.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"数据已成功保存到 weixin_article_data_{i}.json")
                break  # 成功获取数据，跳出重试循环

            except requests.RequestException as e:
                retry_count += 1
                print(f"第{i}个URL - 第{retry_count}次尝试: 请求失败: {e}")
                if retry_count < max_retries:
                    print(f"等待10秒后重试...")
                    time.sleep(10)
                else:
                    print(f"第{i}个URL: 达到最大重试次数，跳过")
            except json.JSONDecodeError as e:
                print(f"第{i}个URL: JSON解析失败: {e}")
                break  # JSON解析失败，不重试

        # 如果重试次数用完仍未成功
        if retry_count >= max_retries:
            print(f"第{i}个URL: 重试{max_retries}次后仍失败，跳过此URL")

        time.sleep(5)  # 避免请求过快导致被封IP


def save_one_data_to_json(url, i):
    """
    将单个URL的数据保存到指定的JSON文件中。

    :param url: 要保存的数据的url
    """
    url = "https://api.tikhub.io/api/v1/wechat_mp/web/fetch_mp_article_detail_json?url=" + url
    headers = {
        'Authorization': 'Bearer 5IJrFWxXgFUUgDsF8kuYwFE0wD1Wtp/Svc9S+gnb8H1My2cnJvIvniKZBA==',
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # 检查请求是否成功

        # 解析JSON响应
        data = response.json()
        # 保存数据到文件
        with open(f'weixin/weixin_articles/weixin_article_data_{i}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"数据已成功保存到 weixin_article_data_{i}.json")
    except requests.RequestException as e:
        print(f"请求失败: {e}")
    except json.JSONDecodeError as e:
        print(f"JSON解析失败: {e}")


# 从指定文件夹中读取所有JSON文件，并将数据解析处理后存入一个列表中
def get_json_data_from_file(file_path):
    """
    从指定的JSON文件中读取数据。

    :param file_path: JSON文件夹的路径
    :return: 解析后的数据
    """
    # 读取文件夹中的所有JSON文件
    if not os.path.exists(file_path):
        print(f"文件 {file_path} 不存在")
        return []
    data_list = []
    for file_name in os.listdir(file_path):
        if file_name.endswith('.json'):
            file_path_full = os.path.join(file_path, file_name)
            try:
                with open(file_path_full, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    data = get_data_from_json(data)
                    if data:  # 确保数据不为空
                        data_list.append(data)
            except json.JSONDecodeError as e:
                print(f"解析文件 {file_path_full} 时出错: {e}")
            except Exception as e:
                print(f"读取文件 {file_path_full} 时出错: {e}")

    return data_list


def get_data_from_json(data):
    """
    从解析后的JSON数据中提取所需信息。

    :param data: 解析后的JSON数据
    :return: 提取后的数据列表
    """
    url = data.get('params', {}).get('url', '')
    data = data.get('data', {})

    if not data:
        print("数据为空或格式不正确")
        return {}

    final_data = {
        'title': data.get('title', ''),
        'url': url,
        'author': data.get('author', ''),
        'author_id': data.get('publish_info', {}).get('user_id', ''),
        'content': data['content']['article'].get('full_text', ''),
        'img_urls': [img.get('src', '') for img in data['content']['article'].get('images', [])],
    }

    return final_data


if __name__ == "__main__":
    # 加载环境变量
    load_dotenv()

    # 从外部获取API密钥
    api_key = os.getenv('TIKHUB_API_KEY')
    if not api_key:
        raise ValueError("请在.env文件中设置TIKHUB_API_KEY")
    # urls = read_urls_from_txt('weixin/url.txt')
    # # print(urls)
    # print(f"读取到 {len(urls)} 个URL")
    # # read_urls_from_xlsx('weixin/url.xlsx', 'weixin/url2.txt')
    # save_data_to_json(urls)

    # 读取指定文件夹中的所有JSON文件

    # folder_path = 'weixin/weixin_articles'
    # all_data = get_json_data_from_file(folder_path)
    # # 存储所有数据到一个新的JSON文件
    # output_file = 'weixin/weixin_all_data.json'
    # with open(output_file, 'w', encoding='utf-8') as f:
    #     json.dump(all_data, f, ensure_ascii=False, indent=2)
    # print(f"所有数据已保存到 {output_file}")
