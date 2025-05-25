import requests
import json
from datetime import datetime
import csv
import time
import os
import random
from crawl_body import crawl_pipeline

# 全局变量定义
count = 0
csv_writer = None


def add_count():
    global count
    count += 1


def get_header():
    with open("weibo_cookie.json", 'r') as f:
        header = json.loads(f.read())
    return header


def decode_base62(b62_str):
    charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    base = 62
    num = 0
    for c in b62_str:
        num = num * base + charset.index(c)
    return num


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


def get_keyword(url):
    list = url.split('/')
    list[-1] = list[-1].split('?')[0]  # 去掉url中的参数部分
    return list[-2], url_to_mid(list[-1])


def get_name(uid):
    url = f"https://weibo.com/ajax/profile/info?custom={uid}"
    return json.loads(requests.get(url=url, headers=get_header()).content.decode('utf-8'))['data']['user']['screen_name']


def get_comment_data(data):
    idstr = data['idstr']
    rootidstr = data['rootidstr']
    created_at = data['created_at']
    dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
    created_at = dt.strftime("%Y-%m-%d %H:%M:%S")
    user_id = data['user']['id']
    text_raw = data['text_raw']
    like = data['like_counts']
    try:
        source = data['source']
        source = source.replace("来自", "")
    except:
        source = "未知"
    try:
        total_number = data['total_number']
    except:
        total_number = 0
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

    return idstr, rootidstr, created_at, user_id, text_raw, like, total_number, gender, source


def get_comment_info(uid, mid, max_id, fetch_level, orig_mid):
    """
    爬取评论信息
    :param uid: 用户ID
    :param mid: 评论或微博ID
    :param max_id: 分页ID
    :param fetch_level: 评论级别(0为一级评论,1为二级评论)
    :param orig_mid: 原始微博ID,用于标识评论所属的微博
    """
    global count, csv_writer
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
            print(f"已爬取到{count}条数据")
            # 随机等待，避免被封
            sleep_time = random.uniform(5, 10)
            print(f"爬取评论-等待 {sleep_time:.2f} 秒...")
            time.sleep(sleep_time)

        idstr, rootidstr, created_at, user_id, text_raw, like, total_number, gender, source = get_comment_data(data)
        if fetch_level == 0:
            rootidstr = ''
        csv_writer.writerow([orig_mid, idstr, rootidstr, user_id, created_at, gender, source, text_raw, like, total_number])
        # 判断是否存在二级评论
        if total_number > 0 and fetch_level == 0:
            get_comment_info(uid, idstr, 0, 1, orig_mid)

    print(f"当前爬取:{count}条")

    # 下一条索引
    max_id = resp['max_id']
    if max_id != 0:
        get_comment_info(uid, mid, max_id, fetch_level, orig_mid)
    else:
        return


def crawl_single_weibo(url):
    """爬取单个微博URL的所有评论"""
    global csv_writer

    try:
        print(f"\n开始爬取: {url}")
        uid, mid = get_keyword(url)
        # print(f"微博ID: {mid}, 用户ID: {uid}")
        # # 尝试获取微博作者名称
        # try:
        #     author_name = get_name(uid)
        #     print(f"作者: {author_name}, 微博ID: {mid}")
        # except:
        #     print(f"微博ID: {mid}, 无法获取作者名称")

        get_comment_info(uid, mid, '', 0, mid)
        print(f"微博 {url} 评论爬取完成")
        return True
    except Exception as e:
        print(f"爬取 {url} 时出错: {str(e)}")
        return False


def batch_crawl_from_file(filepath="weibo_urls.txt", output_file="微博评论汇总.csv"):
    """从文件中读取多个微博URL并批量爬取"""
    global count, csv_writer

    # 重置计数器
    count = 0

    # 创建URL文件(如果不存在)
    if not os.path.exists(filepath):
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("# 每行一个微博URL\n")
            f.write("# 示例: https://www.weibo.com/5393288780/Prnn7nRCg\n")
        print(f"已创建URL列表文件: {filepath}，请在文件中添加微博URL后再运行程序")
        return

    # 读取URL列表
    with open(filepath, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    if not urls:
        print(f"在 {filepath} 中没有找到任何URL，请添加URL后再运行程序")
        return

    # 创建CSV文件并写入表头
    with open(output_file, mode='w', newline='', encoding='utf-8-sig') as file:
        csv_writer = csv.writer(file)
        # 如果已有表头，则不再写入
        if file.tell() == 0:
            # 写入表头
            csv_writer.writerow(['mid', 'review_id', 'sup_comment', 'uid', 'created_at', 'gender', 'source', 'text_raw', 'like', 'review_num'])

        start = time.time()
        total_urls = len(urls)

        print(f"共有 {total_urls} 个微博URL需要爬取")
        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{total_urls}] 正在处理URL: {url}")
            url_start_time = time.time()
            crawl_single_weibo(url)
            url_time = time.time() - url_start_time
            print(f"URL {i}/{total_urls} 爬取完成，耗时 {url_time/60:.2f} 分钟")

            # 在URLs之间稍微暂停，避免频繁请求
            if i < total_urls:
                pause_time = min(5, max(1, url_time * 0.1))  # 暂停时间为爬取时间的10%，最少1秒，最多5秒
                print(f"等待 {pause_time:.1f} 秒后继续下一个URL...")
                time.sleep(pause_time)

    total_time = time.time() - start
    print(f"\n全部爬取完成!")
    print(f"共计爬取了 {total_urls} 个微博，{count} 条评论")
    print(f"总耗时: {total_time/60:.2f} 分钟")
    print(f"评论数据已保存至: {output_file}")


def interactive_mode(mode=2, filepath="weibo_urls.txt", output_file="weibo_details/review_data.csv", one_url="", append=True):
    """交互式模式，允许用户选择爬取方式"""
    # 确保目录存在
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if mode == "1":
        url = one_url
        if url:
            # 检查文件是否存在及是否为空
            file_exists = os.path.exists(output_file)
            file_empty = not file_exists or os.path.getsize(output_file) == 0

            # 根据append参数决定是追加还是覆盖
            file_mode = 'a' if append and file_exists else 'w'

            with open(output_file, mode=file_mode, newline='', encoding='utf-8-sig') as file:
                global csv_writer, count

                # 重置计数器(如果是新文件或不追加)
                if file_mode == 'w':
                    count = 0

                csv_writer = csv.writer(file)

                # 只在文件为空时写入表头
                if file_empty:
                    csv_writer.writerow(['mid', 'review_id', 'sup_comment', 'uid', 'created_at', 'gender', 'source', 'text_raw', 'like', 'review_num'])

                start = time.time()
                crawl_single_weibo(url)
                print(f"爬取完成，共 {count} 条评论，耗时 {(time.time()-start)/60:.2f} 分钟")
                print(f"评论数据已保存至: {output_file}")
    elif mode == "2":
        batch_crawl_from_file(filepath, output_file)
    else:
        print("无效的选择")


if __name__ == "__main__":
    # 读取weibo_urls.txt文件
    with open("weibo_urls.txt", "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    # 创建目录(如果不存在)
    os.makedirs("weibo_details", exist_ok=True)

    # 第一次调用使用覆盖模式，创建新文件
    first_url = False

    for url in urls[124:]:
        # 爬取单个微博URL的所有评论
        interactive_mode(mode="1", one_url=url)

        print(f"等待5秒，避免频繁请求")
        time.sleep(10)

        # 爬取微博详情 - 第一个URL时不追加，之后的URL都追加
        crawl_pipeline([url], append=True)

        # 标记已处理第一个URL
        if first_url:
            first_url = False

    # # 读取一下评论数据集，根据uid算一下到底有多少个用户
    # with open("weibo_details/review_data.csv", "r", encoding="utf-8") as f:
    #     reader = csv.reader(f)
    #     next(reader)
    #     uid_set = set()
    #     for row in reader:
    #         uid_set.add(row[3])
    #     print(f"一共爬取了 {len(uid_set)} 个用户的评论")
