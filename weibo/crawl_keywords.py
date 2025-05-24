"""
微博关键词搜索爬虫
功能：爬取微博关键词搜索结果，获取帖子ID、内容、用户信息等
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


class WeiboSearchCrawler:
    def __init__(self, driver_path='D:\Anaconda\envs\crawl\msedgedriver.exe', headless=False, output_dir="weibo_results3"):
        """
        初始化微博搜索爬虫

        Args:
            driver_path: Edge驱动路径
            headless: 是否使用无头模式
        """
        self.service = Service(executable_path=driver_path)
        self.options = Options()
        self.options.add_argument("--disable-blink-features=AutomationControlled")

        if headless:
            self.options.add_argument('--headless')
            self.options.add_argument('--no-sandbox')
            self.options.add_argument('--disable-dev-shm-usage')

        self.browser = None
        self.output_dir = "weibo_results3"
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
        手动登录微博

        Args:
            timeout: 等待登录的超时时间(秒)

        Returns:
            bool: 登录成功返回True，否则返回False
        """
        self.browser.get("https://weibo.com/login.php")
        print(f"请在{timeout}秒内手动完成登录...")

        try:
            # 等待登录成功，判断依据是页面上出现用户头像元素
            WebDriverWait(self.browser, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".gn_name, .S_txt1"))
            )
            print("登录成功!")
            return True
        except TimeoutException:
            print("登录超时，请重试")
            return False

    def search_keyword(self, keyword, max_pages=5):
        """
        搜索关键词并获取结果

        Args:
            keyword: 搜索关键词
            max_pages: 最大爬取页数

        Returns:
            list: 微博帖子数据列表
        """
        if not self.browser:
            self.start_browser()

        # 构造搜索URL，需要编码关键词
        from urllib.parse import quote
        encoded_keyword = quote(keyword)
        search_url = f"https://s.weibo.com/weibo?q={encoded_keyword}&rd=realtime&tw=realtime&Refer=weibo_realtime"
        # search_url = f"https://s.weibo.com/weibo?q={encoded_keyword}"

        print(f"开始搜索关键词: {keyword}")
        self.browser.get(search_url)

        # 等待页面加载
        time.sleep(3)

        # 检查是否需要登录
        if "login" in self.browser.current_url.lower():
            print("需要登录才能搜索")
            return []

        all_weibos = []

        # 遍历页面
        for page in range(1, max_pages + 1):
            print(f"正在爬取第 {page}/{max_pages} 页")

            try:
                # 等待内容加载
                WebDriverWait(self.browser, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".card-wrap"))
                )

                # 提取当前页的微博列表(排除置顶微博)
                weibo_cards = self.browser.find_elements(By.CSS_SELECTOR, ".card-wrap:not(.card-top)")

                if not weibo_cards:
                    print("未找到微博内容，可能需要登录")
                    break

                page_weibos = []
                print(f"找到 {len(weibo_cards)} 条微博")

                # 遍历微博卡片
                for card in weibo_cards:
                    try:
                        weibo_data = self._parse_weibo_card(card)
                        if weibo_data:
                            page_weibos.append(weibo_data)
                    except Exception as e:
                        print(f"解析微博卡片出错: {e}")
                        continue

                # 将当前页的微博添加到结果中
                all_weibos.extend(page_weibos)

                # 保存当前页数据
                self._save_data(page_weibos, f"{keyword}_page_{page}.json")

                # 判断是否有下一页
                try:
                    next_btn = self.browser.find_element(By.CSS_SELECTOR, ".next")
                    if "disabled" in next_btn.get_attribute("class") or page >= max_pages:
                        print("已到达最后一页或达到最大页数限制")
                        break

                    # 点击下一页
                    next_btn.click()

                    # 随机等待，避免被封
                    sleep_time = random.uniform(5, 10)
                    print(f"等待 {sleep_time:.2f} 秒...")
                    time.sleep(sleep_time)

                except NoSuchElementException:
                    print("找不到下一页按钮，已到达最后一页")
                    break

            except Exception as e:
                print(f"爬取第 {page} 页时出错: {e}")
                break

        print(f"共获取 {len(all_weibos)} 条微博")
        return all_weibos

    def _parse_weibo_card(self, card):
        """解析微博卡片元素，提取微博数据"""
        try:
            # 获取微博ID
            mid = card.get_attribute("mid")

            # 获取用户信息
            user_elem = card.find_element(By.CSS_SELECTOR, ".name")
            user_name = user_elem.text
            user_url = user_elem.get_attribute("href")

            # 获取微博内容
            content = card.find_element(By.CSS_SELECTOR, ".txt").text

            # 获取发布时间
            publish_time_elem = card.find_element(By.CSS_SELECTOR, ".from > a:first-child")
            publish_time = publish_time_elem.text.strip()
            publish_url = publish_time_elem.get_attribute("href")

            # 获取评论数、转发数和点赞数
            stats_elem = card.find_element(By.CSS_SELECTOR, ".card-act")
            stats = stats_elem.find_elements(By.TAG_NAME, "a")
            comment_count = stats[1].text
            repost_count = stats[0].text
            like_count = stats[2].text
            print(f"评论数: {comment_count}, 转发数: {repost_count}, 点赞数: {like_count}")
            # 处理评论数、转发数和点赞数的文本
            if comment_count.isdigit():
                comment_count = int(comment_count)
            else:
                comment_count = 0
            if repost_count.isdigit():
                repost_count = int(repost_count)
            else:
                repost_count = 0
            if like_count.isdigit():
                like_count = int(like_count)
            else:
                like_count = 0
            return {
                "id": mid,
                "user": {
                    "name": user_name,
                    "url": user_url
                },
                "content": content,
                "publish_time": publish_time,
                "publish_url": publish_url,
                "comment_count": comment_count,
                "repost_count": repost_count,
                "like_count": like_count,
            }

        except Exception as e:
            print(f"解析微博卡片时出错: {e}")
            return None

    def _save_data(self, data, filename):
        """保存数据到文件"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"数据已保存到 {filepath}")

    def get_comments(self, weibo_id, max_pages=3):
        """获取指定微博的评论"""
        # 省略实现，如有需要可以补充
        pass

    def __enter__(self):
        self.start_browser()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_browser()


def main(keywords=['None'], cookie_path="cookies.txt", output_dir="weibo_results3"):
    # 使用示例
    keywords = keywords or ["小米SU7高速碰撞爆燃事件细节"]
    cookie_path = cookie_path
    output_dir = output_dir
    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with WeiboSearchCrawler(output_dir=output_dir) as crawler:
        # 加载cookie或手动登录
        if not crawler.load_cookies(cookie_path):
            if crawler.login_manually():
                crawler.save_cookies(cookie_path)

        # 爬取每个关键词
        for keyword in keywords:
            print(f"\n开始爬取关键词: {keyword}")
            results = crawler.search_keyword(keyword, max_pages=50)

            # 打印部分结果
            # if results:
            #     print(f"\n关键词 '{keyword}' 的部分结果:")
            #     for i, weibo in enumerate(results[:3], 1):  # 只显示前3条
            #         print(f"\n微博 {i}:")
            #         print(f"  ID: {weibo['id']}")
            #         print(f"  用户: {weibo['user']['name']}")
            #         content_preview = weibo['content'][:50] + "..." if len(weibo['content']) > 50 else weibo['content']
            #         print(f"  内容: {content_preview}")
            #         print(f"  发布时间: {weibo['publish_time']}")

        # 保存所有结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        crawler._save_data(results, f"all_results_{timestamp}.json")


if __name__ == "__main__":
    main(keywords=["小米SU7高速碰撞爆燃事件细节"],
         cookie_path="cookies.txt",
         output_dir="weibo_results3")
