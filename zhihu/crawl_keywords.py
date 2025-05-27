"""
知乎关键词搜索爬虫
功能：爬取知乎关键词搜索结果，获取文章链接、标题、作者信息等
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


class ZhihuSearchCrawler:
    def __init__(self, driver_path='D:\Anaconda\envs\crawl\msedgedriver.exe', headless=False, output_dir="zhihu_results"):
        """
        初始化知乎搜索爬虫

        Args:
            driver_path: Edge驱动路径
            headless: 是否使用无头模式
            output_dir: 输出目录
        """
        self.service = Service(executable_path=driver_path)
        self.options = Options()
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

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
        手动登录知乎

        Args:
            timeout: 等待登录的超时时间(秒)

        Returns:
            bool: 登录成功返回True，否则返回False
        """
        self.browser.get("https://www.zhihu.com/signin")
        print(f"请在{timeout}秒内手动完成登录...")

        try:
            # 等待登录成功，判断依据是页面上出现用户头像元素
            WebDriverWait(self.browser, timeout).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".Avatar")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".AppHeader-userInfo"))
                )
            )
            print("登录成功!")
            return True
        except TimeoutException:
            print("登录超时，请重试")
            return False

    def scroll_to_load_more(self, scroll_count=10, scroll_pause=4):
        """
        滚动页面加载更多内容

        Args:
            scroll_count: 滚动次数
            scroll_pause: 每次滚动后的等待时间
        """
        print(f"开始滚动加载更多内容，共滚动{scroll_count}次...")

        for i in range(scroll_count):
            # 滚动到页面底部
            self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # 等待内容加载
            time.sleep(scroll_pause)

            # 检查是否有"加载更多"按钮并点击
            try:
                load_more_btn = self.browser.find_element(By.CSS_SELECTOR, ".Button--plain")
                if load_more_btn.is_displayed() and "加载更多" in load_more_btn.text:
                    ActionChains(self.browser).move_to_element(load_more_btn).click().perform()
                    time.sleep(2)
            except NoSuchElementException:
                pass

            print(f"完成第{i+1}次滚动")

    def search_keyword(self, keyword, max_scroll=10):
        """
        搜索关键词并获取文章链接

        Args:
            keyword: 搜索关键词
            max_scroll: 最大滚动次数

        Returns:
            list: 文章数据列表
        """
        if not self.browser:
            self.start_browser()

        # 构造搜索URL
        from urllib.parse import quote
        encoded_keyword = quote(keyword)
        search_url = f"https://www.zhihu.com/search?type=content&q={encoded_keyword}"

        print(f"开始搜索关键词: {keyword}")
        self.browser.get(search_url)

        # 等待页面加载
        time.sleep(3)

        # 检查是否需要登录
        if "/signin" in self.browser.current_url:
            print("需要登录才能搜索")
            return []

        all_articles = []

        try:
            # 等待搜索结果加载
            WebDriverWait(self.browser, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".SearchResult-Card, .List-item"))
            )

            # 滚动加载更多内容
            self.scroll_to_load_more(scroll_count=max_scroll)

            # 获取所有搜索结果
            result_cards = self.browser.find_elements(By.CSS_SELECTOR, ".SearchResult-Card, .List-item")
            # 重复的卡片可能会出现，用set去重
            result_cards = list(set(result_cards))

            if not result_cards:
                print("未找到搜索结果")
                return []

            print(f"找到 {len(result_cards)} 个搜索结果")

            # 解析每个搜索结果
            for i, card in enumerate(result_cards, 1):
                try:
                    article_data = self._parse_article_card(card)
                    if article_data:
                        all_articles.append(article_data)
                        print(f"解析第{i}个结果: {article_data['title'][:50]}...")
                except Exception as e:
                    print(f"解析第{i}个结果时出错: {e}")
                    continue

            # 保存数据
            self._save_data(all_articles, f"{keyword}_articles.json")

        except Exception as e:
            print(f"搜索过程中出错: {e}")

        print(f"共获取 {len(all_articles)} 个搜索结果")
        return all_articles

    def _parse_article_card(self, card):
        """解析搜索结果卡片，提取文章数据"""
        try:
            article_data = {}

            # 获取标题和链接
            title_elem = card.find_element(By.CSS_SELECTOR, "h2 a, .ContentItem-title a")
            article_data['title'] = title_elem.text.strip()
            article_data['url'] = title_elem.get_attribute('href')

            # 获取作者信息
            try:
                author_elem = card.find_element(By.CSS_SELECTOR, ".AuthorInfo-name a, .UserLink-link")
                article_data['author'] = author_elem.text.strip()
                article_data['author_url'] = author_elem.get_attribute('href')
            except NoSuchElementException:
                article_data['author'] = "匿名用户"
                article_data['author_url'] = ""

            # 获取内容摘要
            try:
                content_elem = card.find_element(By.CSS_SELECTOR, ".RichText, .SearchResult-Card .RichText")
                article_data['summary'] = content_elem.text.strip()[:200] + "..." if len(content_elem.text) > 200 else content_elem.text.strip()
            except NoSuchElementException:
                article_data['summary'] = ""

            # 获取点赞数和评论数
            try:
                vote_elem = card.find_element(By.CSS_SELECTOR, ".VoteButton--up .Button-label")
                article_data['vote_count'] = vote_elem.text.strip()
            except NoSuchElementException:
                article_data['vote_count'] = "0"

            try:
                comment_elem = card.find_element(By.CSS_SELECTOR, ".ContentItem-actions button")
                comment_text = comment_elem.text.strip()
                if "评论" in comment_text:
                    article_data['comment_count'] = comment_text.replace("评论", "").strip()
                else:
                    article_data['comment_count'] = "0"
            except NoSuchElementException:
                article_data['comment_count'] = "0"

            # 获取发布时间
            try:
                time_elem = card.find_element(By.CSS_SELECTOR, ".ContentItem-time")
                article_data['publish_time'] = time_elem.text.strip()
            except NoSuchElementException:
                article_data['publish_time'] = ""

            # 获取文章类型（问答、文章、视频等）
            try:
                type_elem = card.find_element(By.CSS_SELECTOR, ".SearchResult-Card--type")
                article_data['content_type'] = type_elem.get_attribute('data-za-extra-module') or "文章"
            except NoSuchElementException:
                article_data['content_type'] = "文章"

            return article_data

        except Exception as e:
            print(f"解析文章卡片时出错: {e}")
            return None

    def _save_data(self, data, filename):
        """保存数据到文件"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"数据已保存到 {filepath}")

    def get_article_content(self, article_url):
        """获取文章详细内容"""
        try:
            self.browser.get(article_url)
            time.sleep(3)

            # 获取文章内容
            content_elem = self.browser.find_element(By.CSS_SELECTOR, ".RichText")
            return content_elem.text.strip()
        except Exception as e:
            print(f"获取文章内容失败: {e}")
            return ""

    def __enter__(self):
        self.start_browser()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_browser()


def main(keywords=None, cookie_path="zhihu_cookies.json", output_dir="zhihu_results", max_scroll=10):
    """
    主函数

    Args:
        keywords: 搜索关键词列表
        cookie_path: cookies文件路径
        output_dir: 输出目录
        max_scroll: 最大滚动次数
    """
    keywords = keywords or ["人工智能"]

    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with ZhihuSearchCrawler(output_dir=output_dir) as crawler:
        # 尝试加载cookies，如果失败则手动登录
        crawler.browser.get("https://www.zhihu.com")
        time.sleep(2)

        if not crawler.load_cookies(cookie_path):
            if crawler.login_manually():
                crawler.save_cookies(cookie_path)
            else:
                print("登录失败，将以游客身份继续（可能会有限制）")

        all_results = []

        # 爬取每个关键词
        for keyword in keywords:
            print(f"\n开始爬取关键词: {keyword}")
            results = crawler.search_keyword(keyword, max_scroll=max_scroll)
            all_results.extend(results)

            # 打印部分结果
            if results:
                print(f"\n关键词 '{keyword}' 的部分结果:")
                for i, article in enumerate(results[:3], 1):  # 只显示前3条
                    print(f"\n文章 {i}:")
                    print(f"  标题: {article['title']}")
                    print(f"  作者: {article['author']}")
                    print(f"  链接: {article['url']}")
                    print(f"  点赞数: {article['vote_count']}")
                    print(f"  评论数: {article['comment_count']}")

            # 随机等待，避免被封
            if len(keywords) > 1:
                sleep_time = random.uniform(5, 10)
                print(f"等待 {sleep_time:.2f} 秒...")
                time.sleep(sleep_time)

        # 保存所有结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        crawler._save_data(all_results, f"all_zhihu_results_{timestamp}.json")


if __name__ == "__main__":
    main(
        keywords=["小米SU7高速爆燃事件细节"],
        cookie_path="zhihu/zhihu_cookie.json",
        output_dir="zhihu/zhihu_results",
        max_scroll=15
    )
