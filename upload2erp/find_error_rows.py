# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/5/27 11:17
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import pymysql
import re
import numpy as np
import warnings
import zipfile
import time
import shutil
import rsa
import base64
import requests
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import sys
from my_toolkit import process_files


# 定时刷新网页
def refresh_url(url, time_interval=60, start_time=0, end_time=24,
                chromedriver_path=r"D:\pycharmproject\venv\Scripts\chromedriver.exe"):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)
    i = 1
    flag = 0
    while 1:
        try:
            driver.get(url)
            time.sleep(time_interval)
            # print('fresh successful.')
            flag = 1
            break
        except:
            pass
        i += 1
        if i >= 4:
            break
    driver.close()
    if flag == 1:
        return True
    else:
        return False




# 通过接口将新增和更新数据上传的erp上
class AdApiFileUpload(object):
    """
    广告组广告上传的接口:
        分为：新建和更新
        新建的接口为:(线下) 'http://192.168.2.160:80/services/api/advertise/Creatrreportinterface'
                参数： account_id和upload_file
        更新接口: url:http://192.168.2.168:80/services/api/advertise/updatereport
                参数:account_id和data

    """

    # __base_url = 'http://192.168.2.160:80'  # 线下环境
    __base_url = 'http://120.78.243.154'  # 线上环境
    __create_url = '/services/api/advertise/Creatrreportinterface'
    __update_url = '/services/api/advertise/updatereport'
    __key_path = r"C:\Users\Administrator\Desktop\广告文档与图片\public.key"

    def __init__(self):
        pass

    def __token__(self):
        # 通过接口上传数据
        with open(self.__key_path, 'r') as fp:
            public_key = fp.read()
        # pkcs8格式
        key = public_key
        password = "Kr51wGeDyBM39Q0REVkXn4lW7ZqCxdPLS8NO6iIfubTJcvsjt2YpmAgzHFUoah"
        pubkey = rsa.PublicKey.load_pkcs1_openssl_pem(key)
        password = password.encode('utf8')
        crypt_password = rsa.encrypt(password, pubkey)
        token = base64.b64encode(crypt_password).decode()
        return token

    # 通过接口上传新建的文件
    def requests_upload_create(self, **kwargs):
        """
        通过上传需要新增的站点对应的站点的id和站点的路径，将文件通过api上传到服务器中
        :param kwargs: 'account_id'、'file_path'
        :return:
        """
        token = self.__token__()
        account_id = int(kwargs['account_id'])
        file_path = kwargs['file_path']
        # 读取文件
        with open(file_path, 'rb') as f:
            content = f.read()
        # print(content)

        upload_create_url = self.__base_url + self.__create_url
        # 普通参数
        upload_param = {
            'token': token,
            'account_id': account_id,
            'upload_file': open(file_path, 'rb')
        }
        # print(f'站点id {account_id} 开始新增上传.')
        files = {'upload_file': open(file_path, 'rb')}

        response = requests.post(upload_create_url, data=upload_param, files=files)
        status_code = response.status_code
        # print(f'status_code: {response.status_code}')
        if status_code == 200:
            info = json.loads(response.content)['info']
            if info == '上传成功':
                return True
            else:
                return False
        else:
            return

    # 通过接口上传更新的文件
    def requests_upload_update(self, **kwargs):
        """
        通过接口参数上传站点的更新文件
        :param kwargs: account_id和file_path
        :return:
        """
        token = self.__token__()
        # 更新接口的account_id 是站点名 账号+下划线+站点
        account_id = kwargs['account_id']
        file_path = kwargs['file_path']
        with open(file_path, 'rb') as f:
            content = f.read()
        # url
        update_url = self.__base_url + self.__update_url
        # 参数
        upload_param = {
            'token': token,
            'account_id': account_id,
            'data': content
        }
        print(f'站点 {account_id} 开始更新上传.')
        response = requests.post(update_url, data=upload_param)
        status_code = response.status_code
        print(f'status_code: {response.status_code}')
        if status_code == 200:
            print(response.text)
        else:
            print(f'状态:fail.')


        # erp_upload_refresh_url = "http://120.78.243.154/services/advertising/generatereport/generatereport"
        # is_refresh = refresh_url(erp_upload_refresh_url, time_interval=60)
        # # print(is_refresh)
        # # upload_status = False
        # if not is_refresh:
        #     raise ValueError(f'无法刷新页面:{erp_upload_refresh_url}')




if __name__ =="__main__":
    account_id = 1134
    requests_upload = AdApiFileUpload()
    folder = r"C:\Users\Administrator\Desktop\上传测试"
    files_list = os.listdir(folder)
    # file_list = [os.path.join(folder,file) for file in files_list]
    file_list = [r"C:\Users\Administrator\Desktop\YOLINA_ES_2020-05-27_all_create_data_rest_row.xlsx"]
    for path in file_list:
        print(path)
        upload_create_dict = {
            'account_id': account_id,
            'file_path': path
        }
        upload_status = requests_upload.requests_upload_create(**upload_create_dict)
        # 刷新页面上传
        erp_upload_refresh_url = "http://120.78.243.154/services/advertising/generatereport/generatereport"
        is_refresh = refresh_url(erp_upload_refresh_url, time_interval=5)


