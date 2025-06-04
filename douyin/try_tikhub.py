# 导入异步io库 | Import asyncio
import asyncio
import json
import os
import requests
import time
from dotenv import load_dotenv


if __name__ == "__main__":
    # 加载环境变量
    load_dotenv()

    # 从外部获取API密钥
    api_key = os.getenv('TIKHUB_API_KEY')
    if not api_key:
        raise ValueError("请在.env文件中设置TIKHUB_API_KEY")
    # 获取单个作品数据 | Get single video data
    # video_data = asyncio.run(client.DouyinWeb.fetch_video_comments(aweme_id="7510518369545358650",cursor=0, count=40))
    # 保存数据到文件 | Save data to file
    url = "https://api.tikhub.io/api/v1/douyin/search/fetch_video_search_v1"
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer ' + api_key,
        'Content-Type': 'application/json'
    }

    search_id = ""
    for i in range(20):
        times = 0

        data = {
            "keyword": "小米su7高速爆燃",
            "cursor": i*8,
            "sort_type": "0",
            "publish_time": "0",
            "filter_duration": "0",
            "content_type": "0",
            "search_id": search_id,
        }

        response = requests.post(url, headers=headers, json=data)
        # 检查响应状态 | Check response status
        while response.status_code != 200 and times < 5:
            print(f"请求失败，状态码: {response.status_code}, 重试次数: {times + 1}")
            time.sleep(2)
            response = requests.post(url, headers=headers, json=data)
            times += 1
        # 解析响应内容 | Parse response content
        video_data = response.json()

        # search_id = video_data.get("data", {}).get("extra", {}).get("logid", "")
        # print(f"第 {i} 次请求成功，新的search_id为: {search_id}")
        with open(f"douyin/douyin_results_tikhub_new/video_data_3_{i}.json", "w", encoding="utf-8") as f:
            # video_data 是 一个字典 | video_data is a dictionary
            json.dump(video_data, f, ensure_ascii=False, indent=2)
        # 输出提示信息 | Print a message
        print(f"数据已保存到 video_data_3_{i}.json")  # 输出提示信息 | Print a message
        if video_data.get("data", {}).get("has_more", 0) == 0:
            print(video_data['data']['has_more'])
            print(f"第 {i} 次请求没有更多数据，结束循环")
            break
        time.sleep(5)
