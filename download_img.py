import requests

#下载图片
# https://wx3.sinaimg.cn/orj1080/006H8U51gy1i1e63awha6j30u00mi77t.jpg

# 下载
def download_image(url, save_path):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            print(f"图片已保存到 {save_path}")
        else:
            print(f"下载失败，状态码: {response.status_code}")
    except Exception as e:
        print(f"下载图片失败: {e}")

# 测试
if __name__ == "__main__":
    image_url = "https://wx3.sinaimg.cn/orj1080/006H8U51gy1i1e63awha6j30u00mi77t.jpg"
    save_path = "downloaded_image.jpg"
    download_image(image_url, save_path)