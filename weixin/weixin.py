import http.client

conn = http.client.HTTPSConnection("api.tikhub.io")
payload = ''
headers = {"Authorization": "Bearer 5IJrFWxXgFUUgDsF8kuYwFE0wD1Wtp/Svc9S+gnb8H1My2cnJvIvniKZBA=="}
conn.request("GET", "https://api.tikhub.io/api/v1/wechat_mp/web/fetch_mp_article_detail_json?url=https://mp.weixin.qq.com/s/sYMzPTYFMVPrzp7nAPcCBg", payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))