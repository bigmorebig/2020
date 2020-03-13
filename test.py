import requests


url = "https://www.baidu.com/"
r = requests.get(url)
print(r.text)
