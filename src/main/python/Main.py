import urllib.request
from selenium import webdriver
import DomPath
from bs4 import BeautifulSoup
import time
from urllib.parse import quote
import json
from pykafka import KafkaClient


def geturl(url):
    head = {}
    head[
        'User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'
    res = urllib.request.Request(url, None, head)
    response = urllib.request.urlopen(res)
    html = response.read()
    return html


def getResponse(url):
    data = geturl(url).decode('utf-8')
    return data


def htmlToidAndTitle(trs):
    list = []
    for tr in trs:
        news = tr.find_all('td')[1].find('div').find('div').find('div').find('span')
        id = news.find('a').get('href').split('?')[1][3:]
        title = news.find('a').find('b').get('title')
        map = {"id": id, "title": title}
        list.append(map)
    return list

# kafka-topics.sh --create --zookeeper bay1:2181,bay2:2181,bay3:2181 --replication-factor 3 --partitions 3 --topic sparkStreamWordSeq

host = "bay1:9092,bay2:9092,bay3:9092"
client = KafkaClient(hosts=host)
topic = 'sparkStreamWordSeq'
topicdocu = client.topics[topic]
producer = topicdocu.get_producer()

def tojieba(text):
    url = "http://localhost:8282/token/" + quote(text)
    response = getResponse(url)
    json_loads = json.loads(response)
    print(response)
    print(json_loads["terms"])
    producer.produce(json_loads["terms"].encode())


# 进入网易云音乐的框架获取框架代码
def get_ids(url):
    chrome_options = webdriver.ChromeOptions()
    # 使用headless无界面浏览器模式
    chrome_options.add_argument('--headless')  # 增加无界面选项
    chrome_options.add_argument('--disable-gpu')  # 如果不加这个选项，有时定位会出现问题
    paths = DomPath.get_config()
    browser = webdriver.Chrome(paths['Chrome'])
    allcomments = []
    try:
        browser.get(url)
        time.sleep(1)
        browser.switch_to_frame(browser.find_element_by_id("g_iframe"))
        browser.find_element_by_xpath('//tbody//div[@class="ttc"]')
        html = browser.page_source
        bs = BeautifulSoup(html, "html.parser")
        trs = bs.find('tbody').find_all('tr')
        items = htmlToidAndTitle(trs)
        print(items)
        for index, item in enumerate(items):
            if (index > 1):
                break
            url2 = "https://music.163.com/#/song?id="
            id = item.get("id")
            browser.get(url2 + id)
            time.sleep(1)
            browser.switch_to_frame(browser.find_element_by_id("g_iframe"))
            html = browser.page_source
            bs = BeautifulSoup(html, "html.parser")
            select = bs.select(".cmmts.j-flag")[0]
            bs = BeautifulSoup(str(select), "html.parser")
            lines = bs.find("div")
            comments = []
            for index, line in enumerate(lines):
                soup = BeautifulSoup(str(line), "html.parser")
                # print(soup)
                itm = soup.select(".itm")
                if len(itm) >= 1:
                    userid, context = "", ""
                    div = BeautifulSoup(str(itm[0]), "html.parser").find("div")
                    userHref = div.find("div").find("a").get("href")
                    userid = str(userHref).split("?id=")[1]
                    info = div.find_all("div", limit=2)[1]
                    context = BeautifulSoup(str(info), "html.parser").find("div").find("div").find("div").text
                    comment = {"id": id, "userid": userid, "context": context}
                    comments.append(comment)
                    tojieba(context.split("：")[-1])
        allcomments.append(comments)
    finally:
        browser.stop_client()
        browser.close()
    return allcomments


if __name__ == '__main__':
    url = "https://music.163.com/#/discover/toplist?id=3778678"
    item = get_ids(url)
    producer.stop()
