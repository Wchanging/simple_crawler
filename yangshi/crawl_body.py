"""
央视网文章内容爬虫
功能：根据URL列表爬取文章详情，包括标题、发布时间、内容、图片等
"""
import requests
from bs4 import BeautifulSoup
import json
import time
import os
import csv
from datetime import datetime
from urllib.parse import urljoin


def parse_article_detail(url):
    """
    解析单篇文章的详细信息

    Args:
        url: 文章URL

    Returns:
        dict: 文章详情字典，包含标题、发布时间、内容、图片等
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }

    try:
        print(f"正在爬取: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = 'utf-8'

        if response.status_code != 200:
            print(f"请求失败，状态码: {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        # 查找 page_body
        page_body = soup.find('div', id='page_body')
        if not page_body:
            print(f"未找到page_body容器")
            return None

        # 提取标题 - 在title_area中的h1标签
        title = ""
        title_area = page_body.find('div', class_='title_area')
        if title_area:
            h1_tag = title_area.find('h1')
            if h1_tag:
                title = h1_tag.get_text(strip=True)

        # 提取副标题 - h2标签
        subtitle = ""
        h2_tag = page_body.find('h2', id='subtitle')
        if h2_tag:
            subtitle = h2_tag.get_text(strip=True)

        # 提取发布信息 - info1 div
        publish_info = ""
        publish_time = ""
        source = ""
        info_div = page_body.find('div', class_='info')
        if info_div:
            publish_info = info_div.get_text(strip=True)
            # 尝试提取时间
            info_text = info_div.get_text()
            if '|' in info_text:
                parts = info_text.split('|')
                if len(parts) >= 2:
                    publish_time = parts[1].strip()

        # 提取来源标题 - h6标签
        source_title = ""
        h6_tag = page_body.find('h6', id='sourcetitle')
        if h6_tag:
            source_title = h6_tag.get_text(strip=True)

        # 提取正文内容 - content_area中的所有段落和图片
        content_paragraphs = []
        images = []

        content_area = page_body.find('div', class_='content_area')
        if content_area:
            # 提取所有段落文本
            paragraphs = content_area.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text and text not in ['<!--repaste.body.begin-->', '']:
                    content_paragraphs.append(text)

            # 提取所有图片
            img_tags = content_area.find_all('p', class_=lambda x: x and 'photo_img' in x)
            for img_p in img_tags:
                img_tag = img_p.find('img')
                if img_tag:
                    img_url = img_tag.get('src', '')
                    # 如果是相对路径，转换为绝对路径
                    if img_url and not img_url.startswith('http'):
                        img_url = urljoin(url, img_url)

                    img_alt = img_tag.get('alt', '')
                    img_width = img_tag.get('width', '')

                    if img_url:
                        images.append(str(img_url))

            # 提取图片说明文字
            photo_alt_tags = content_area.find_all('p', class_='photo_alt')
            img_descriptions = []
            for alt_p in photo_alt_tags:
                desc = alt_p.get_text(strip=True)
                if desc:
                    img_descriptions.append(desc)

        # 整合所有内容
        full_content = '\n'.join(content_paragraphs)

        article_data = {
            'url': url,
            'title': title,
            'subtitle': subtitle,
            'publish_info': publish_info,
            'publish_time': publish_time,
            'source_title': source_title,
            'content': full_content,
            'image_count': len(images),
            'img_urls': str(images)
        }

        print(f"✓ 成功获取: {title}")
        print(f"  - 内容长度: {len(full_content)} 字符")
        print(f"  - 段落数: {len(content_paragraphs)}")
        print(f"  - 图片数: {len(images)}")

        return article_data

    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")
        return None
    except Exception as e:
        print(f"解析错误: {e}")
        return None


def save_to_json(articles, output_file):
    """
    保存文章数据到JSON文件

    Args:
        articles: 文章列表
        output_file: 输出文件路径
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"JSON数据已保存到: {output_file}")


def save_to_csv(articles, output_file):
    """
    保存文章数据到CSV文件

    Args:
        articles: 文章列表
        output_file: 输出文件路径
    """
    if not articles:
        print("没有数据可保存")
        return

    # CSV字段（不包含复杂的嵌套数据）
    fieldnames = ['url', 'title', 'subtitle', 'publish_info', 'publish_time',
                  'source_title', 'content', 'image_count', 'img_urls']

    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for article in articles:
            # 只写入基本字段，跳过images字段
            row = {k: v for k, v in article.items() if k in fieldnames}
            writer.writerow(row)

    print(f"CSV数据已保存到: {output_file}")


def crawl_articles_from_urls(url_list, output_dir="yangshi_articles", save_interval=5, delay=2):
    """
    批量爬取文章列表

    Args:
        url_list: URL列表
        output_dir: 输出目录
        save_interval: 每隔多少篇文章保存一次JSON文件
        delay: 每篇文章之间的延迟时间(秒)

    Returns:
        list: 文章数据列表
    """
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    articles = []
    total = len(url_list)

    # 准备文件路径
    json_file = os.path.join(output_dir, "all_articles.json")
    csv_file = os.path.join(output_dir, "all_articles.csv")

    # 初始化CSV文件（写入表头）- 包含图片URL字段
    csv_fieldnames = ['url', 'title', 'subtitle', 'publish_info', 'publish_time',
                      'source_title', 'content', 'image_count', 'img_urls']

    with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
        writer.writeheader()

    # 初始化JSON文件
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump([], f, ensure_ascii=False, indent=2)

    print(f"开始爬取 {total} 篇文章...")
    print(f"JSON保存间隔: 每 {save_interval} 篇")
    print(f"CSV文件: 每篇立即保存")
    print(f"{'='*60}\n")

    success_count = 0
    fail_count = 0

    for i, url in enumerate(url_list, 1):
        print(f"[{i}/{total}] ", end='')

        article = parse_article_detail(url)

        if article:
            articles.append(article)
            success_count += 1

            # 立即追加到CSV文件
            with open(csv_file, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
                row = {k: v for k, v in article.items() if k in csv_fieldnames}
                writer.writerow(row)

            # 定期更新JSON文件
            if i % save_interval == 0 or i == total:
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(articles, f, ensure_ascii=False, indent=2)
                print(f"  → JSON已更新 ({len(articles)} 篇)\n")
        else:
            fail_count += 1

        # 延迟
        if i < total:
            time.sleep(delay)

    # 最终更新JSON文件（确保完整性）
    if articles:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)

        print(f"\n{'='*60}")
        print(f"爬取完成!")
        print(f"成功: {success_count}/{total} 篇")
        print(f"失败: {fail_count} 篇")
        print(f"文章数据(CSV)已保存到: {csv_file}")
        print(f"文章数据(JSON)已保存到: {json_file}")
        print(f"{'='*60}")

    return articles


def crawl_from_json_file(json_file_path, output_dir="yangshi_articles", save_interval=5, delay=2):
    """
    从JSON文件中读取URL列表并爬取

    Args:
        json_file_path: JSON文件路径(由crawl_keywords.py生成)
        output_dir: 输出目录
        save_interval: 每隔多少篇文章保存一次
        delay: 每篇文章之间的延迟时间(秒)
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 支持两种格式
        if 'urls' in data:
            urls = data['urls']
        elif 'unique_urls' in data:
            urls = data['unique_urls']
        else:
            print("JSON文件格式不正确，找不到urls或unique_urls字段")
            return

        print(f"从 {json_file_path} 读取到 {len(urls)} 个URL")
        return crawl_articles_from_urls(urls, output_dir, save_interval, delay)

    except FileNotFoundError:
        print(f"文件不存在: {json_file_path}")
    except json.JSONDecodeError:
        print(f"JSON文件格式错误: {json_file_path}")
    except Exception as e:
        print(f"读取文件出错: {e}")


def convert_json_to_csv(json_file_path, output_csv_path):
    """
    将文章JSON文件转换为CSV文件

    Args:
        json_file_path: 输入JSON文件路径
        output_csv_path: 输出CSV文件路径
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            articles = json.load(f)

        if not articles:
            print("没有数据可转换")
            return

        # CSV字段（不包含复杂的嵌套数据）
        # 从json数据中提取所有可能的字段
        fieldnames = set()
        for article in articles:
            fieldnames.update(article.keys())
        fieldnames = list(fieldnames)
        # 字段排序
        fieldnames.sort()
        with open(output_csv_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for article in articles:
                row = {k: v for k, v in article.items() if k in fieldnames}
                writer.writerow(row)
        print(f"CSV数据已保存到: {output_csv_path}")
    except FileNotFoundError:
        print(f"文件不存在: {json_file_path}")
    except json.JSONDecodeError:
        print(f"JSON文件格式错误: {json_file_path}")
    except Exception as e:
        print(f"读取文件出错: {e}")


if __name__ == "__main__":
    # 方式1: 直接提供URL列表
    # test_urls = [
    #     "https://news.cctv.com/2025/10/17/ARTI5bYuHS5GXbQMGR6mn7Jr251017.shtml",
    # ]
    # crawl_articles_from_urls(test_urls, output_dir="yangshi/yangshi_articles", save_interval=5, delay=2)

    # 方式2: 从crawl_keywords.py生成的JSON文件读取URL
    # json_file = "yangshi/yangshi_results/all_unique_urls.json"
    # crawl_from_json_file(json_file, output_dir="yangshi/yangshi_articles", save_interval=5, delay=2)

    # 方式3: 从去重后的URL文件读取
    # json_file = "yangshi/yangshi_results/all_unique_urls.json"
    # crawl_from_json_file(json_file, output_dir="yangshi/yangshi_articles", save_interval=10, delay=2)

    convert_json_to_csv(
        json_file_path="yangshi/yangshi_articles/all_articles.json",
        output_csv_path="yangshi/yangshi_articles/all_articles.csv"
    )
