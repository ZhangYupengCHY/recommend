import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re

"""
获取网页中某一标签值
"""


def connect_url(url):
    rsp = requests.get(url)
    html_doc = rsp.text
    return BeautifulSoup(html_doc, 'html.parser')


def parse_label_value(html_info, label_name):
    """
    获取标签中的值
    :param html_info:html
        网页内容
    :param label_name:str
        标签内容
    :return: list
        返回值
    """
    all_label_info = html_info.find_all(label_name)
    all_label_value = []
    for label in all_label_info:
        label_value = label.get_text()
        all_label_value.append(label_value)
    return all_label_value


def get_label_value(url, label_name):
    """
    获取某一标签中的值:
        1.连接网页
        2.提取/解析内容
    :return:
    """
    all_html_value = connect_url(url)

    label_value = parse_label_value(all_html_value, label_name)
    return label_value


if __name__ == '__main__':
    url = 'http://app.finance.ifeng.com/hq/stock_weekly.php?code=sh000001'
    # 获取表头
    th_label_name = 'th'
    th_value = get_label_value(url, th_label_name)
    print(th_value)
    # 获取表数据
    td_label_name = 'td'
    td_value = get_label_value(url, td_label_name)
    print(td_value)

