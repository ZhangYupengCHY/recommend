# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/5/27 9:17
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os


# 定时刷新网页
def refresh_url(url, time_interval, start_time=0, end_time=24,
            chromedriver_path=r"D:\pycharmproject\venv\Scripts\chromedriver.exe"):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)
    driver.get(url)
    while 1:
        time.sleep(time_interval)
        try:
            driver.refresh()
            print('fresh successful.')
        except:
            print('fresh fail.')
    driver.close()


if __name__ == "__main__":
    # url = "http://120.78.243.154/services/advertising/generatereport/generatereport"
    # refresh_url(url, 60)

    path = os.path.split(os.path.abspath( ))[0]
    project_basename = os.getcwd()
    print(os.path.relpath(__file__))