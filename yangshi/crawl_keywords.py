"""
央视网关键词搜索爬虫
功能：爬取央视网关键词搜索结果，获取文章URL
"""
import requests
from bs4 import BeautifulSoup
import time
import json
import os
from urllib.parse import quote
import random


def get_article_urls(keyword, max_pages=5, output_dir="yangshi_results", save_interval=1):
    """
    批量获取央视网搜索结果中的文章URL

    Args:
        keyword: 搜索关键词
        max_pages: 最大爬取页数，默认为5
        output_dir: 输出目录，默认为"yangshi_results"
        save_interval: 每隔多少页保存一次，默认为1(每页都保存)。设置为5表示每5页保存一次

    Returns:
        list: 文章URL列表
    """
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 编码关键词
    encoded_keyword = quote(keyword)

    all_urls = []

    # 设置请求头，模拟浏览器
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }

    print(f"开始爬取关键词: {keyword}")

    # 准备增量保存的文件路径
    all_urls_file = os.path.join(output_dir, f"{keyword}_all_urls.txt")
    json_file = os.path.join(output_dir, f"{keyword}_all_urls.json")

    # 如果文件已存在，先清空（或者可以选择追加模式）
    with open(all_urls_file, 'w', encoding='utf-8') as f:
        f.write(f"{'='*60}\n")
        f.write(f"关键词: {keyword}\n")
        f.write(f"开始时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*60}\n\n")

    # 初始化JSON文件
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump({
            'keyword': keyword,
            'start_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_count': 0,
            'urls': []
        }, f, ensure_ascii=False, indent=2)

    for page in range(1, max_pages + 1):
        # 构造搜索URL
        search_url = f"https://search.cctv.com/search.php?qtext={encoded_keyword}&page={page}&type=web&sort=date&datepid=1&channel=&vtime=-1&is_search=1"

        print(f"正在爬取第 {page}/{max_pages} 页: {search_url}")

        try:
            # 发送请求
            response = requests.get(search_url, headers=headers, timeout=10)
            response.encoding = 'utf-8'

            # 检查响应状态
            if response.status_code != 200:
                print(f"请求失败，状态码: {response.status_code}")
                continue

            # 解析HTML
            soup = BeautifulSoup(response.text, 'html.parser')

            # 找到 <div class="ind03">
            ind03_div = soup.find('div', class_='ind03')

            if not ind03_div:
                print(f"第 {page} 页未找到搜索结果容器")
                continue

            # 找到所有的 <li class="image">
            image_items = ind03_div.find_all('li', class_='image')

            if not image_items:
                print(f"第 {page} 页未找到任何搜索结果")
                break

            print(f"第 {page} 页找到 {len(image_items)} 条结果")

            page_urls = []

            # 遍历每个li元素
            for item in image_items:
                try:
                    # 找到包含lanmu1属性的span标签
                    span = item.find('span', attrs={'lanmu1': True})

                    if span and span.get('lanmu1'):
                        url = span.get('lanmu1')
                        page_urls.append(url)
                        print(f"  获取URL: {url}")

                except Exception as e:
                    print(f"  解析单个结果时出错: {e}")
                    continue

            # 添加到总结果
            all_urls.extend(page_urls)

            # 始终保存到TXT文件(因为是追加模式,不耗费太多IO)
            with open(all_urls_file, 'a', encoding='utf-8') as f:
                f.write(f"{'─'*60}\n")
                f.write(f"第 {page} 页 (共 {len(page_urls)} 条URL)\n")
                f.write(f"{'─'*60}\n")
                for url in page_urls:
                    f.write(url + '\n')
                f.write('\n')  # 添加空行

            print(f"第 {page} 页的 {len(page_urls)} 条URL已增量保存到 {all_urls_file}")

            # 根据save_interval参数决定是否保存JSON文件
            # 在以下情况保存: 1) 达到保存间隔 2) 最后一页 3) 出现错误前的最后一页
            should_save_json = (page % save_interval == 0) or (page == max_pages) or (not image_items)

            if should_save_json:
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'keyword': keyword,
                        'start_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                        'current_page': page,
                        'total_count': len(all_urls),
                        'urls': all_urls
                    }, f, ensure_ascii=False, indent=2)

                print(f"JSON文件已更新 (当前共 {len(all_urls)} 条URL)")

            # 随机等待，避免请求过快
            if page < max_pages:
                wait_time = random.uniform(2, 3)
                print(f"等待 {wait_time} 秒后继续...")
                time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            print(f"请求第 {page} 页时出错: {e}")
            # 出错时也保存一次JSON,避免数据丢失
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'keyword': keyword,
                    'start_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'current_page': page,
                    'error': str(e),
                    'total_count': len(all_urls),
                    'urls': all_urls
                }, f, ensure_ascii=False, indent=2)
            print(f"发生错误,已保存当前数据到JSON文件")
            continue
        except Exception as e:
            print(f"处理第 {page} 页时出错: {e}")
            # 出错时也保存一次JSON,避免数据丢失
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'keyword': keyword,
                    'start_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'current_page': page,
                    'error': str(e),
                    'total_count': len(all_urls),
                    'urls': all_urls
                }, f, ensure_ascii=False, indent=2)
            print(f"发生错误,已保存当前数据到JSON文件")
            continue

    # 在文件末尾添加总结信息
    if all_urls:
        with open(all_urls_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"爬取完成!\n")
            f.write(f"结束时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"共获取 {len(all_urls)} 条URL\n")
            f.write(f"{'='*60}\n")

        print(f"\n共获取 {len(all_urls)} 条URL，已保存到 {all_urls_file}")

        # 最终更新JSON文件,添加完成状态
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump({
                'keyword': keyword,
                'start_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'end_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'completed',
                'total_count': len(all_urls),
                'urls': all_urls
            }, f, ensure_ascii=False, indent=2)
        print(f"JSON格式已保存到 {json_file}")
    else:
        print("\n未获取到任何URL")

    return all_urls


def batch_get_article_urls(keywords, max_pages=5, output_dir="yangshi_results", save_interval=1):
    """
    批量获取多个关键词的搜索结果URL

    Args:
        keywords: 关键词列表
        max_pages: 每个关键词最大爬取页数
        output_dir: 输出目录
        save_interval: 每隔多少页保存一次JSON文件，默认为1(每页都保存)

    Returns:
        dict: 每个关键词对应的URL列表
    """
    results = {}

    for keyword in keywords:
        print(f"\n{'='*60}")
        print(f"开始处理关键词: {keyword}")
        print(f"{'='*60}")

        urls = get_article_urls(keyword, max_pages, output_dir, save_interval)
        results[keyword] = urls

        # 每个关键词之间等待一段时间
        if keyword != keywords[-1]:
            wait_time = 3
            print(f"\n等待 {wait_time} 秒后处理下一个关键词...")
            time.sleep(wait_time)

    # 对所有URL进行去重
    all_urls_combined = []
    for urls in results.values():
        all_urls_combined.extend(urls)

    unique_urls = list(set(all_urls_combined))  # 去重
    duplicate_count = len(all_urls_combined) - len(unique_urls)

    # 保存去重后的URL列表到JSON文件
    unique_urls_json_file = os.path.join(output_dir, "all_unique_urls.json")
    with open(unique_urls_json_file, 'w', encoding='utf-8') as f:
        json.dump({
            'keywords': keywords,
            'results': {k: len(v) for k, v in results.items()},
            'total_urls': len(all_urls_combined),
            'unique_urls_count': len(unique_urls),
            'duplicate_count': duplicate_count,
            'unique_urls': unique_urls  # 排序后保存
        }, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"所有关键词处理完成!")
    print(f"总URL数: {len(all_urls_combined)}")
    print(f"去重后URL数: {len(unique_urls)}")
    print(f"重复URL数: {duplicate_count}")
    print(f"去重URL列表(JSON)已保存到 {unique_urls_json_file}")
    print(f"{'='*60}")

    return results


if __name__ == "__main__":
    # 测试单个关键词
    # keyword = "俄乌"
    # urls = get_article_urls(keyword, max_pages=30, output_dir="yangshi/yangshi_results", save_interval=5)

    # 测试多个关键词
    # save_interval=5 表示每5页保存一次JSON文件,可以减少IO操作,提高效率
    keywords = ["俄乌", "俄乌战争", "俄乌冲突"]
    results = batch_get_article_urls(keywords, max_pages=30, output_dir="yangshi/yangshi_results", save_interval=5)
