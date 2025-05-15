# https://weibo.com/ajax/statuses/buildComments?flow=1&is_reload=1&id=5153820120452372&is_show_bulletin=2&is_mix=1&fetch_level=1&max_id=0&count=20&uid=2397417584&locale=zh-CN
# https://weibo.com/ajax/statuses/buildComments?flow=1&is_reload=1&id=5153820120452372&is_show_bulletin=2&is_mix=1&fetch_level=1&max_id=5153822667443438&count=20&uid=2397417584&locale=zh-CN


import requests
import json
from datetime import datetime
import csv
import time

# 统计评论数量
count = 0


def add_count():
    global count
    count += 1


# 获取标头
def get_header():
    with open("weibo_cookie.json", 'r') as f:
        header = json.loads(f.read())
    return header


# 提取url中的关键词
def get_keyword(url):
    list = url.split('/')
    return list[-2], url_to_mid(list[-1])


# 解码
def decode_base62(b62_str):
    charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    base = 62
    num = 0
    for c in b62_str:
        num = num * base + charset.index(c)
    return num


# 将url转换为mid
# 通过将url分段，每段进行base62解码，最后拼接成完整的mid
# 例如：url = "Prnn7nRCg" -> mid = 5153820120452372
def url_to_mid(url):
    result = ''
    for i in range(len(url), 0, -4):
        start = max(i - 4, 0)
        segment = url[start:i]
        num = str(decode_base62(segment))
        if start != 0:
            num = num.zfill(7)  # 除最后一段外都补满7位
        result = num + result
    return int(result)


# 根据UID返回博主的用户名
def get_name(uid):
    url = f"https://weibo.com/ajax/profile/info?custom={uid}"
    return json.loads(requests.get(url=url, headers=get_header()).content.decode('utf-8'))['data']['user']['screen_name']


# 解析评论json数据并返回
def get_comment_data(data):
    # 评论ID
    idstr = data['idstr']
    # 上级评论ID
    rootidstr = data['rootidstr']
    # 发表日期
    created_at = data['created_at']
    dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
    created_at = dt.strftime("%Y-%m-%d %H:%M:%S")
    # 用户名
    screen_name = data['user']['screen_name']
    # 用户ID
    user_id = data['user']['id']
    # 评论内容
    text_raw = data['text_raw']
    # 评论点赞数
    like = data['like_counts']
    # 评论回复数量
    try:
        total_number = data['total_number']
    except:
        total_number = 0
    # 性别
    try:
        gender = data['user']['gender']
        if gender == 'm':
            gender = '男'
        elif gender == 'f':
            gender = '女'
        else:
            gender = '未知'
    except:
        gender = '未知'

    return idstr, rootidstr, created_at, user_id, screen_name, text_raw, like, total_number, gender


# 返回评论数据
def get_comment_info(uid, mid, max_id, fetch_level):
    if max_id == '':
        url = f"https://weibo.com/ajax/statuses/buildComments?flow=1&is_reload=1&id={mid}&is_show_bulletin=2&is_mix=0&count=20&uid={uid}&fetch_level={fetch_level}&locale=zh-CN"
    else:
        url = f"https://weibo.com/ajax/statuses/buildComments?flow=1&is_reload=1&id={mid}&is_show_bulletin=2&is_mix=0&max_id={max_id}&count=20&uid={uid}&fetch_level={fetch_level}&locale=zh-CN"

    resp = json.loads(requests.get(url=url, headers=get_header()).content.decode('utf-8'))
    datas = resp['data']

    for data in datas:
        add_count()
        # 每爬取100条数据，等待5秒，防止反爬干扰
        if count % 100 == 0:
            print(f"已爬取到{count}条数据，防止爬取过快，等待5秒......")
            time.sleep(5)

        idstr, rootidstr, created_at, user_id, screen_name, text_raw, like, total_number, gender = get_comment_data(data)
        if fetch_level == 0:
            rootidstr = ''
        csv_writer.writerow([count, idstr, rootidstr, user_id, created_at, screen_name, gender, text_raw, like, total_number])
        # 判断是否存在二级评论
        if total_number > 0 and fetch_level == 0:
            get_comment_info(uid, idstr, 0, 1)

    print(f"当前爬取:{count}条")

    # 下一条索引
    max_id = resp['max_id']
    if max_id != 0:
        get_comment_info(uid, mid, max_id, fetch_level)
    else:
        return

if __name__ == "__main__":

    # 统计爬虫运行时间
    start = time.time()

    url = "https://www.weibo.com/5393288780/Prnn7nRCg"

    uid, mid = get_keyword(url)
    # 创建CSV文件并写入表头
    with open(f"评论(Min版)w.csv", mode='w', newline='', encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(['序号', '评论标识号', '上级评论', '用户标识符', '时间', '用户名', '性别', '评论内容', '评论点赞数', '评论回复数'])
        get_comment_info(uid, mid, '', 0)

    print(f"评论爬取完成，共计{count}条，耗时{(time.time()-start)/60:.2f}分")
