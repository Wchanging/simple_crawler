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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Cookie': '_xsrf=8xcyHVSLEJ2ibQ1y1lIWHnImvZHUmbLw; _zap=f0f63903-2845-46d3-a393-363bfc6ed7bc; d_c0=2eSTEHPuXhqPTuNkQaTEpPr7_0JMyxEsPuc=|1745827340; captcha_session_v2=2|1:0|10:1745827341|18:captcha_session_v2|88:T3hyWHdMbVFzUXQ3WE93VHhPQktoWnhMWTE2L2o3R3Z3WWRMQVpvdjY2WW5qZXNXNjcxcnhpZUFCUUdJbEV5RA==|bb1f38c9f47d5bce6b09742da17d30af08a6f9523011a402aead6a7f8d67082d; captcha_ticket_v2=2|1:0|10:1745827348|17:captcha_ticket_v2|728:eyJ2YWxpZGF0ZSI6IkNOMzFfLnB2X0ZfVzZiVThfMW0qZ0hBblloa3hnUGpsNVQ1aEZ2X3JKZUZoVzhPaXkxRWsuSWRMVC41WEVFYVBvMi5kYnEybHF4LjFhSFlSWGhqRjlyUlhoMXhuSFFubnRlajZuX0l4MEhod2ZFd041bGtXS29RWklRUXAqMTFlb1BhWHJvVWp4VFhIRERSWEw5OTVRcXdrbjJlMzhITEF2QlJpQUg2aktsSlFfeHN4VVVDTG01VnQ2T3NlekFzSjFQT0xIazQ0Vjhmd3hLQ2hUcFgxajJaLkFlVDNxaVVtOTBwNTF5UHMwRDJGVlFOQnBsZjBIYzFINm41TFZmWFFqdVFSTVdRYXpiVFhfNmdHR1VUX1FJNnpXVk9sanV6QU1iTnpjREJsWGpUNSpEcWlWazQ0ZCpUZipEV0NkblU2cnZTWGNkTFFqWVZRTTl4ME9GdFZOVkdoZ3ZVZWlfb0VBWWhBZDI4Q1hfd2VkV2RKRXJDNWlJMXlmUkJsTUQ1U1p6MXNZSWZSeE1tT1hFclljdkhzS3dlUEw1T3NXWkN0aG5TZTBWNGFyZjlDZ3J4dDNPV3kucHFZQ3A1MUtrMkwyWW9vWVMwVWdvWHlJRlZqYVVDUnd5ODFuTjJkbVBjbndvcjlKOFhITzNhMVB5VjQuSk9rLlE5T2g2d0pQS2thTjRsd2VtYlIzMWc3N182X2lfMSJ9|d07d9dfb31b1d632c7b1360c74fd1cda6b5206f482741d79662ad743b41945ff; q_c1=5186950f62414d4496666b3eeb539dee|1746006904000|1746006904000; z_c0=2|1:0|10:1746332328|4:z_c0|80:MS4xaXVUWlJBQUFBQUFtQUFBQVlBSlZUYWc0QkduZnRGdF9pcHg4bzFCbWk1dUQ5SGx1bUFTUEZBPT0=|bb6ae5775bf99266862ca4e3f6f2b3439b9fcd6b63662b3f008499c2ad4b1142; edu_user_uuid=edu-v1|408292aa-ab62-40b7-a368-290ceed9d8c6; __zse_ck=004_NNY47p4O5s=R4GzL9juO5BapJPF3=B7Bnjfv3icsWfw6AT6STyNB6xS3mwnews7qde2WxSjmx2o8MxwMeRMW62eB16ezVGxY84F63NrcVdlBOgT/GfeJg8jcUd1yiCI0-vRgeJRxJiPNmn+MoBZgFGRBRqAWbqZYLdV8Ffshxd5EGcm3U1yf/T1oDfySQ4DXcq0oSxnPjCwh0vQBpmk3U0dCBatJk5V+SSZCcGGFfhhZvRp4ohb6CvJneO4QwiqgSBXGQKSk84IIObF1B4SNbatvYEtNoBTLKlm4uNofPmbw=; BEC=6c53268835aec2199978cd4b4f988f8c; tst=r; SESSIONID=I1tyqTSIWnMQLqqdUw9PpP7Yk89h40VpykYMPpuzg6A'}
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
                print(f"  有效图片 {valid_count}: {img_url}")
            elif img_url:
                print(f"  过滤无效图片: {img_url[:100]}...")  # 只显示前100个字符

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
