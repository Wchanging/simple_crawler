import requests
import json
import os
import time
import random
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import csv
from datetime import datetime


def get_page(url):
    """
    获取页面内容
    :param url: 页面url
    :return: 页面内容
    """
    with open('zhihu/zhihu_cookie_.json', 'r', encoding='utf-8') as f:
            headers = json.load(f)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # print(response.text)
        return response.text
    else:
        print(f"Failed to retrieve page: {url}")
        return None


def detect_url_type(url):
    """
    检测URL类型
    :param url: 页面URL
    :return: 'article' 或 'answer'
    """
    if 'zhuanlan.zhihu.com' in url:
        return 'article'
    elif 'zhihu.com/question' in url and 'answer' in url:
        return 'answer'
    else:
        return 'unknown'


def is_valid_image_url(img_url):
    """
    检查图片URL是否有效
    :param img_url: 图片URL
    :return: 布尔值，True表示有效
    """
    if not img_url:
        return False

    # 过滤掉data:image/svg+xml格式的占位符图片
    if img_url.startswith('data:image/svg+xml'):
        return False

    # 过滤掉data:开头的其他base64图片（通常是占位符）
    if img_url.startswith('data:'):
        return False

    # 过滤掉非常小的图片（可能是图标或占位符）
    # 可以根据URL中的尺寸参数判断
    if 'width' in img_url and 'height' in img_url:
        try:
            # 提取width和height参数
            import re
            width_match = re.search(r'width[\'"]?\s*[:=]\s*[\'"]?(\d+)', img_url)
            height_match = re.search(r'height[\'"]?\s*[:=]\s*[\'"]?(\d+)', img_url)
            if width_match and height_match:
                width = int(width_match.group(1))
                height = int(height_match.group(1))
                # 过滤掉尺寸太小的图片（如小于50x50的图标）
                if width < 50 or height < 50:
                    return False
        except:
            pass

    # 确保是有效的图片URL
    valid_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
    valid_domains = ['zhimg.com', 'pic.zhimg.com', 'picx.zhimg.com', 'pic1.zhimg.com']

    # 检查域名
    from urllib.parse import urlparse
    parsed_url = urlparse(img_url)
    domain_valid = any(domain in parsed_url.netloc for domain in valid_domains)

    # 检查文件扩展名或URL特征
    extension_valid = any(ext in img_url.lower() for ext in valid_extensions)

    return domain_valid and extension_valid


def parse_page(html, url):
    """
    解析页面内容
    :param html: 页面内容
    :param url: 页面URL，用于判断页面类型
    :return: 解析后的数据
    """
    soup = BeautifulSoup(html, 'html.parser')
    data = []

    # 根据URL类型选择不同的解析策略
    url_type = detect_url_type(url)
    print(f"检测到URL类型: {url_type}")

    if url_type == 'answer':
        # 回答页面的解析逻辑
        post_container = soup.find('div', class_='RichContent RichContent--unescapable')
        # 只取第一个div
        if post_container:
            post_container = post_container.find('div', class_='RichContent-inner')
            print("使用回答页面解析逻辑")
    elif url_type == 'article':
        # 文章页面的解析逻辑
        post_container = soup.find('div', class_='Post-RichTextContainer')
        print("使用文章页面解析逻辑")
    else:
        # 未知类型，尝试两种方式
        print("未知URL类型，尝试两种解析方式")
        post_container = soup.find('div', class_='Post-RichTextContainer')
        if not post_container:
            post_container = soup.find('div', class_='RichContent RichContent--unescapable')
            if post_container:
                post_container = post_container.find('div', class_='RichContent-inner')

    if post_container:
        print("找到内容容器，开始提取图片")
        # 找到所有的img标签
        img_tags = post_container.find_all('img')

        # 用于去重的集合
        seen_urls = set()
        valid_count = 0

        for img in img_tags:
            # 尝试多种可能的图片URL属性
            img_url = (img.get('src') or
                       img.get('data-original') or
                       img.get('data-src') or
                       img.get('data-actualsrc'))

            # 检查URL是否有效且未重复
            if img_url and is_valid_image_url(img_url) and img_url not in seen_urls:
                seen_urls.add(img_url)
                data.append({
                    'title': 'Image',
                    'link': img_url,
                    'url_type': url_type,
                    'alt': img.get('alt', ''),  # 添加alt属性
                    'width': img.get('width', ''),  # 添加宽度
                    'height': img.get('height', '')  # 添加高度
                })
                valid_count += 1
                # print(f"  有效图片 {valid_count}: {img_url}")
            elif img_url:
                # print(f"  过滤无效图片: {img_url[:100]}...")  # 只显示前100个字符
                pass

        print(f"共找到 {len(img_tags)} 个img标签，其中 {valid_count} 张有效图片")
    else:
        print("未找到内容容器")

    return data


if __name__ == "__main__":
    # 测试不同类型的URL
    test_urls = [
        "https://zhuanlan.zhihu.com/p/638414676",  # 文章
        "https://zhuanlan.zhihu.com/p/668885759",  # 文章
        "https://www.zhihu.com/question/621464361/answer/3303824536",  # 回答
        "https://www.zhihu.com/question/660068650/answer/3544103528"   # 回答
    ]

    # 选择要测试的URL
    url = test_urls[0]  # 可以修改索引来测试不同的URL

    print(f"正在处理URL: {url}")
    html = get_page(url)
    if html:
        data = parse_page(html, url)
        print(f"\n提取结果:")
        for i, item in enumerate(data, 1):
            print(f"{i}. Title: {item['title']}, Link: {item['link']}, Type: {item['url_type']}")
            if item['alt']:
                print(f"   Alt: {item['alt']}")
    else:
        print("无法获取页面内容")
