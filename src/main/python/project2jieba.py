# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 13:06:51 2020

@author: hp
"""
import jieba  # 导入包
import re

cut = jieba.cut
# bottle 是一个http 的开发工具
from bottle import route, run


# sentence :待拆分的字符串    中华人民共和国  ->
# 返回值:  以空格分隔的   字符串
def token(sentence):
    seg_list = list(cut(sentence))
    return " ".join(seg_list)


# 路由设置
@route('/token/:sentence')
def index(sentence):
    sentence = re.sub(r'[a-zA-Z]', '1', sentence)
    print("====", sentence)
    result = token(sentence)
    return "{\"ret\":0, \"msg\":\"OK\", \"terms\":\"%s\"}" % result


# 相当于  java 中的   main

if __name__ == "__main__":
    # 以      http://localhost:8282/token/今天是星期天       访问
    run(host="localhost", port=8282)
