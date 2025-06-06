import requests


# 下载微博图片
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
    image_url = f"https://p3-sign.douyinpic.com/tos-cn-i-p14lwwcsbr/06872a350ce8475e92fec279f96b4760~tplv-p14lwwcsbr-7.image?lk3s=7b078dd2&x-expires=1748966400&x-signature=ohUnIGY%2F3I346Y8RjCxyIRYAV4E%3D&from=2064092626&se=false&sc=image&biz_tag=aweme_comment&l=20250603183549C2793F67DCBC39BDC431"
    save_path = "downloaded_image.jpg"
    download_image(image_url, save_path)
