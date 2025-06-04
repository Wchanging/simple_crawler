import requests
import json
from datetime import datetime
import csv
import time
import random
import os


def get_weibo_data(data):
    # 微博ID
    mid = data['idstr']
    # 作者ID
    uid = data['user']['idstr']
    # 作者url
    user_url = 'https://www.weibo.com' + data['user']['profile_url']
    # 微博内容
    text = data['text_raw']
    # 发布时间
    created_at = data['created_at']
    dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")  # 将字符串转换为datetime对象
    created_at = int(dt.timestamp())  # 转换为时间戳
    # 地区
    try:
        region = data['region_name']
        region = region.replace("发布于 ", "")
        region = region if region else "未知"
    except:
        region = "未知"
    # 图片数量
    pic_num = data['pic_num']
    # 图片url
    pic_url = []
    video_url = []
    if pic_num == 0:
        pic_url = []
        video_url = []
    else:
        # 如果存在data['mix_media_info']，则为包含视频和图片的微博
        if 'mix_media_info' in data:
            pic_url = []
            video_url = []
            for media in data['mix_media_info']['items']:
                if media['type'] == "pic":
                    pic_url.append(media['data']['original']['url'])
                elif media['type'] == "video":
                    video_url.append(media['data']['media_info']['h5_url'])
        else:
            if 'pic_infos' in data:
                video_url = []
                for media in data['pic_infos']:
                    pic_url.append(data['pic_infos'][media]['original']['url'])
            elif 'page_info' in data:
                if data['page_info']['object_type'] == 'video':
                    video_url.append(data['page_info']['short_url'])

    # 评论数
    comments_count = data['comments_count']
    # 转发数
    reposts_count = data['reposts_count']
    # 点赞数
    like_count = data['attitudes_count']
    return mid, uid, user_url, text, created_at, pic_num, pic_url, video_url, comments_count, reposts_count, like_count, region


def get_header():
    with open("weibo/weibo_cookie.json", 'r') as f:
        header = json.loads(f.read())
    return header


def change_url(original_url):
    # 将url  https://www.weibo.com/7455753652/Pralx8mbj?refer_flag=1001030103_ 转换为 https://www.weibo.com/ajax/statuses/show?id=Pralx8mbj&locale=zh-CN&isGetLongText=true
    # 1. 获取url中的关键词
    # 2. 拼接新的url
    # 3. 返回新的url
    list = original_url.split('/')
    mid = list[-1].split('?')[0]
    new_url = f"https://www.weibo.com/ajax/statuses/show?id={mid}&locale=zh-CN&isGetLongText=true"
    return new_url


def crawl_pipeline(urls, file_name='weibo_details/meta_data.csv', append=True):
    """
    爬取微博详情并保存到CSV文件

    Args:
        urls: 微博URL列表
        file_name: 保存文件名
        append: 是否追加模式，True为追加，False为覆盖
    """
    if not urls:
        print("没有要爬取的微博URL")
        return

    # 创建目录(如果不存在)
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    # 确定文件打开模式和是否需要写入表头
    file_exists = os.path.exists(file_name)
    write_header = not file_exists or not append
    file_mode = 'a' if append and file_exists else 'w'

    # 结果保存在csv文件中
    with open(file_name, file_mode, newline='', encoding='utf-8') as csvfile:
        fieldnames = ['mid', 'uid', 'text', 'created_at', 'region', 'pic_num', 'pic_url', 'video_url',
                      'comments_count', 'reposts_count', 'like_count', 'original_url', 'user_url']
        writer = csv.writer(csvfile)

        # 如果需要写入表头(新文件或覆盖模式)
        if write_header:
            writer.writerow(fieldnames)

        # 遍历每个url
        for original_url in urls:
            # 获取微博数据
            url = change_url(original_url)
            resp = requests.get(url=url, headers=get_header())
            resp = json.loads(resp.content.decode('utf-8'))
            mid, uid, user_url, text, created_at, pic_num, \
                pic_url, video_url, comment_count, repost_count, like_count, region = get_weibo_data(resp)
            # 写入csv文件
            writer.writerow([mid, uid, text, created_at, region, pic_num, pic_url, video_url,
                            comment_count, repost_count, like_count, original_url, user_url,])

            # 每爬一条微博，随机等待2-5秒，防止反爬
            print(f"爬取微博ID: {mid} 成功，等待下一条...")
            sleep_time = random.uniform(4, 5)
            print(f"等待 {sleep_time:.2f} 秒...")
            time.sleep(sleep_time)


if __name__ == "__main__":
    # 打开weibo_results2/merged.json文件
    # with open("weibo_results2/merged.json", "r", encoding="utf-8") as f:
    #     data = json.load(f)
    #     urls = []
    #     for item in data:
    #         # 获取微博url
    #         original_url = item['publish_url']
    #         # 转换为新的url
    #         urls.append(original_url)
    # # print(urls)

    # 爬取微博数据
    # crawl_pipeline(['https://weibo.com/1314608344/PllANsMlp?refer_flag=1001030103_'])
    # print(f"微博数据已保存到weibo_details/meta_data.csv")

    # 看看爬取的结果有多少条
    with open("weibo_details/meta_data.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        # 读取表头
        header = next(reader)
        # 统计行数
        count = sum(1 for row in reader)
        print(f"爬取到 {count} 条微博数据")
    # 关闭文件
    f.close()

    # # 读取微博数据中的图片url
    # with open("weibo_details/meta_data.csv", "r", encoding="utf-8") as f:
    #     reader = csv.reader(f)
    #     # 读取表头
    #     header = next(reader)
    #     # 输出图片url
    #     for row in reader:
    #         # 获取图片url
    #         pic_url = row[6]
    #         print(type(pic_url))
    #         # 将字符串转换为列表
    #         pic_url = pic_url.strip("[]").replace("'", "").split(", ")
    #         # 输出图片url
    #         for url in pic_url:
    #             print(url)
    # # 关闭文件
    # f.close()
