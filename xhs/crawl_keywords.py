"""
小红书关键词搜索爬虫
功能：爬取小红书关键词搜索结果，获取笔记链接
"""
import time
import json
import random
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains


class XiaohongshuSearchCrawler:
    def __init__(self, driver_path='D:\Anaconda\envs\crawl\msedgedriver.exe', headless=False, output_dir="xiaohongshu_results"):
        """
        初始化小红书搜索爬虫

        Args:
            driver_path: Edge驱动路径
            headless: 是否使用无头模式
            output_dir: 输出目录
        """
        self.service = Service(executable_path=driver_path)
        self.options = Options()
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # 添加小红书特定的反检测设置
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)

        if headless:
            self.options.add_argument('--headless')
            self.options.add_argument('--no-sandbox')
            self.options.add_argument('--disable-dev-shm-usage')

        self.browser = None
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def start_browser(self):
        """启动浏览器"""
        if not self.browser:
            print("启动浏览器...")
            self.browser = webdriver.Edge(service=self.service, options=self.options)
            # 执行反检测脚本
            self.browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.browser.maximize_window()

    def close_browser(self):
        """关闭浏览器"""
        if self.browser:
            print("关闭浏览器...")
            self.browser.quit()
            self.browser = None

    def load_cookies(self, cookie_path):
        """
        从文件加载cookies

        Args:
            cookie_path: cookies文件路径

        Returns:
            bool: 加载成功返回True，否则返回False
        """
        try:
            with open(cookie_path, 'r') as f:
                cookies = json.load(f)
                for cookie in cookies:
                    self.browser.add_cookie(cookie)
            print("成功加载cookies")
            return True
        except Exception as e:
            print(f"加载cookies失败: {e}")
            return False

    def save_cookies(self, cookie_path):
        """
        保存cookies到文件

        Args:
            cookie_path: 保存路径
        """
        try:
            cookies = self.browser.get_cookies()
            with open(cookie_path, 'w') as f:
                json.dump(cookies, f)
            print(f"cookies已保存到 {cookie_path}")
        except Exception as e:
            print(f"保存cookies失败: {e}")

    def login_manually(self, timeout=60):
        """
        手动登录小红书

        Args:
            timeout: 等待登录的超时时间(秒)

        Returns:
            bool: 登录成功返回True，否则返回False
        """
        self.browser.get("https://www.xiaohongshu.com/explore")
        print(f"请在{timeout}秒内手动完成登录...")

        try:
            # 等待登录成功，判断依据是页面上出现用户头像元素
            WebDriverWait(self.browser, timeout).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".avatar")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".user-info")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-v-]"))
                )
            )
            print("登录成功!")
            return True
        except TimeoutException:
            print("登录超时，将继续以游客身份访问")
            return False

    def scroll_to_load_more(self, scroll_count=10, scroll_pause=3):
        """
        滚动页面加载更多内容

        Args:
            scroll_count: 滚动次数
            scroll_pause: 每次滚动后的等待时间

        Returns:
            list: 累积收集的所有URL
        """
        print(f"开始滚动加载更多内容，共滚动{scroll_count}次...")

        all_urls = set()  # 使用set自动去重

        for i in range(scroll_count):
            # 获取滚动前的页面高度
            last_height = self.browser.execute_script("return document.body.scrollHeight")

            # 滚动到页面底部
            self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # 等待内容加载
            time.sleep(scroll_pause)

            # 额外等待，确保动态内容加载完成
            new_height = self.browser.execute_script("return document.body.scrollHeight")
            wait_count = 0
            while new_height == last_height and wait_count < 3:
                time.sleep(2)
                new_height = self.browser.execute_script("return document.body.scrollHeight")
                wait_count += 1

            # 检查是否有加载更多的指示器
            try:
                loading_elements = self.browser.find_elements(By.CSS_SELECTOR, ".loading, .load-more, [class*='loading']")
                if loading_elements:
                    time.sleep(2)
            except NoSuchElementException:
                pass

            # 提取当前页面的所有URL并添加到集合中
            current_urls = self._extract_note_urls()
            for url in current_urls:
                all_urls.add(url)

            print(f"完成第{i+1}次滚动，本次找到 {len(current_urls)} 个链接，累积 {len(all_urls)} 个不重复链接")

            # 如果连续几次滚动都没有新内容，可能已经到底了
            if new_height == last_height:
                print(f"页面高度未变化，可能已加载完所有内容")

        return list(all_urls)

    def search_keyword(self, keyword, max_scroll=10):
        """
        搜索关键词并获取笔记链接

        Args:
            keyword: 搜索关键词
            max_scroll: 最大滚动次数

        Returns:
            list: 笔记URL列表
        """
        if not self.browser:
            self.start_browser()

        # 构造搜索URL
        from urllib.parse import quote
        encoded_keyword = quote(keyword)
        search_url = f"https://www.xiaohongshu.com/search_result?keyword={encoded_keyword}&source=web_search_result_notes"

        print(f"开始搜索关键词: {keyword}")
        print(f"搜索URL: {search_url}")
        self.browser.get(search_url)

        # 等待页面加载
        time.sleep(5)

        note_urls = []

        try:
            # 等待搜索结果加载
            print("等待搜索结果加载...")
            WebDriverWait(self.browser, 15).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/search_result/']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".note-item")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='note']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/explore/']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".feeds-page"))
                )
            )
            print("搜索结果已加载")

            # 先获取滚动前的链接数量
            initial_urls = self._extract_note_urls()
            print(f"滚动前找到 {len(initial_urls)} 个链接")

            # 滚动加载更多内容并累积收集URL
            note_urls = self.scroll_to_load_more(scroll_count=max_scroll)

            # 过滤并确保都是小红书链接
            note_urls = [url for url in note_urls if url and 'xiaohongshu.com' in url]

            print(f"最终收集到 {len(note_urls)} 个笔记链接")

            # 保存数据
            self._save_urls(note_urls, f"{keyword}_notes_urls.json")

        except Exception as e:
            print(f"搜索过程中出错: {e}")
            # 即使出错也尝试提取已有的链接
            try:
                note_urls = self._extract_note_urls()
                note_urls = [url for url in note_urls if url and 'xiaohongshu.com' in url]
                print(f"异常处理中提取到 {len(note_urls)} 个链接")
            except:
                pass

        return note_urls

    def _extract_note_urls(self):
        """提取页面中的笔记URL"""
        note_urls = []

        try:
            # 更详细的调试信息
            print("开始提取笔记URL...")

            # 更新选择器，专门针对小红书搜索结果页面的笔记链接
            selectors = [
                "a[href*='/explore/']",         # 备用格式
                "a[href*='/search_result/']",  # 主要的笔记链接格式
                "a.cover",                     # 封面链接
                "a.mask",                      # 带遮罩的链接
                ".note-item a",               # 笔记项目链接
                "section a[href*='/search_result/']"  # section下的搜索结果链接
            ]

            for selector in selectors:
                try:
                    elements = self.browser.find_elements(By.CSS_SELECTOR, selector)
                    print(f"选择器 '{selector}' 找到 {len(elements)} 个元素")

                    for elem in elements:
                        href = elem.get_attribute('href')
                        if href and ('/search_result/' in href or '/explore/' in href):
                            # 确保是完整的URL
                            if not href.startswith('http'):
                                href = 'https://www.xiaohongshu.com' + href
                            note_urls.append(href)

                    if note_urls:
                        print(f"使用选择器 {selector} 成功提取到 {len(note_urls)} 个链接")
                        break

                except Exception as e:
                    print(f"选择器 {selector} 执行失败: {e}")
                    continue

            # 如果上述方法都失败，尝试从页面源码中提取
            if not note_urls:
                print("CSS选择器未找到链接，尝试从页面源码中提取...")
                import re
                page_source = self.browser.page_source

                # 更精确的正则表达式
                patterns = [
                    r'href="(/search_result/[a-f0-9]+[^"]*)"',  # 带引号的完整href
                    r'/search_result/[a-f0-9]+(?:\?[^"\s]*)?',   # search_result格式
                    r'/explore/[a-f0-9]+(?:\?[^"\s]*)?',         # explore格式（备用）
                ]

                for pattern in patterns:
                    matches = re.findall(pattern, page_source)
                    print(f"正则表达式 '{pattern}' 找到 {len(matches)} 个匹配")

                    for match in matches:
                        # 如果匹配的是相对路径，需要补全
                        if match.startswith('/'):
                            full_url = 'https://www.xiaohongshu.com' + match
                        else:
                            full_url = 'https://www.xiaohongshu.com/' + match
                        note_urls.append(full_url)

                    if note_urls:
                        print(f"使用正则表达式成功提取到 {len(note_urls)} 个链接")
                        break

            # 最后的调试信息
            print(f"_extract_note_urls 函数总共提取到 {len(note_urls)} 个URL")
            if note_urls:
                print("前3个URL示例:")
                for i, url in enumerate(note_urls[:3], 1):
                    print(f"  {i}: {url}")

        except Exception as e:
            print(f"提取笔记URL时出错: {e}")

        return note_urls

    def _save_urls(self, urls, filename):
        """保存URL列表到文件"""
        filepath = os.path.join(self.output_dir, filename)
        data = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'count': len(urls),
            'urls': urls
        }
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"URL列表已保存到 {filepath}")

    def __enter__(self):
        self.start_browser()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_browser()


def main(keywords=None, cookie_path="xiaohongshu_cookies.json", output_dir="xiaohongshu_results", max_scroll=15):
    """
    主函数

    Args:
        keywords: 搜索关键词列表
        cookie_path: cookies文件路径
        output_dir: 输出目录
        max_scroll: 最大滚动次数
    """
    keywords = keywords or ["美食"]

    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with XiaohongshuSearchCrawler(output_dir=output_dir) as crawler:
        # 尝试加载cookies，如果失败则手动登录
        crawler.browser.get("https://www.xiaohongshu.com/explore")
        time.sleep(3)

        if not crawler.load_cookies(cookie_path):
            if crawler.login_manually():
                crawler.save_cookies(cookie_path)
            else:
                print("未登录，将以游客身份继续（可能会有限制）")

        all_urls = []

        # 爬取每个关键词
        for keyword in keywords:
            print(f"\n开始爬取关键词: {keyword}")
            urls = crawler.search_keyword(keyword, max_scroll=max_scroll)
            all_urls.extend(urls)

            # 打印部分结果
            if urls:
                print(f"\n关键词 '{keyword}' 找到的笔记URL:")
                for i, url in enumerate(urls[:5], 1):  # 只显示前5条
                    print(f"  {i}: {url}")

            # 随机等待，避免被封
            if len(keywords) > 1:
                sleep_time = random.uniform(8, 15)
                print(f"等待 {sleep_time:.2f} 秒...")
                time.sleep(sleep_time)

        # 保存所有结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        all_urls = list(set(all_urls))  # 去重
        crawler._save_urls(all_urls, f"all_xiaohongshu_urls_{timestamp}.json")

        print(f"\n总共获取到 {len(all_urls)} 个不重复的笔记链接")


if __name__ == "__main__":
    main(
        keywords=["小米su7高速爆燃事件"],
        cookie_path="xhs/xhs_cookies.json",
        output_dir="xhs/xhs_results",
        max_scroll=15
    )
