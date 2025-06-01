# 导入异步io库 | Import asyncio
import asyncio
import json
import os

# 导入tikhub | Import tikhub
from tikhub import Client

# 初始化Client | Initialize Client
client = Client(base_url="https://api.tikhub.io", 
                api_key="y5IvEk5N3VMpgDUu62fghF8E1yYBrML+saIr0BHMiD1420g+S8Jy6iC2Xw==")

if __name__ == "__main__":
    # 获取单个作品数据 | Get single video data
    video_data = asyncio.run(client.DouyinWeb.fetch_video_comments(aweme_id="7510518369545358650",cursor=0, count=40))
    # 保存数据到文件 | Save data to file
    with open("video_data.json", "w", encoding="utf-8") as f:
        # video_data 是 一个字典 | video_data is a dictionary
        json.dump(video_data, f, ensure_ascii=False, indent=4)
    # 输出提示信息 | Print a message
    print("数据已保存到 video_data.json")  # 输出提示信息 | Print a message