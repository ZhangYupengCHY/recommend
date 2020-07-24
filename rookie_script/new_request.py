# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 11:31:40 2019

@author: Administrator
"""
import requests
import win32api
import os
import zipfile
from datetime import datetime
import datetime as dt
import numpy as np
import pandas as pd
import shutil
import re
import time
import json
import logging
import threading


def process_one_camp(camp_site):
    request_types = ['campaign', 'business', 'search']
    base_dir = zip_dir + '{}'.format(camp_site).upper() + '_{}'.format(today_date)
    if not os.path.exists(base_dir):
        os.mkdir(base_dir)
    all_reports_list = []
    for request_type in request_types:
        #  times = 0
        site = (camp_site[-2:]).upper()
        account = (camp_site[:-3]).upper()
        report_end_date = datetime.now().date()
        report_start_date = (report_end_date - dt.timedelta(days=30))
        if request_type in ['campaign', 'search']:
            try:
                file_list = write_files(site=site, account=account, report_start_date=report_start_date,
                            report_end_date=report_end_date, type=request_type)
                all_reports_list.extend(file_list)
            except:
                # logging.exception("\tTYPE1: DO NOT HAVE {}_{} request_result {}".format(account, site))
                pass
        else:
            try:
                file_list = write_files(site=site, account=account, type=request_type)
                all_reports_list.extend(file_list)
            except:
                # logging.exception("\tTYPE1: DO NOT HAVE {}_{} request_result {}".format(account, site))
                pass
    all_threads2 = []
    t3 = datetime.now()
    for file in all_reports_list:
        one_thread = threading.Thread(target=request_url,
                                      kwargs={'file_name': file, 'account': account, 'site': site})
        all_threads2.append(one_thread)
    for my_thread in all_threads2:
        my_thread.start()
    for my_thread in all_threads2:
        my_thread.join()
    t4 = datetime.now()
    print("请求花费:{}".format(t4-t3))
    one_info = read_reports(site=site, account=account)
    with open(r'C:\Users\Administrator\Desktop\{}_new_api_request_part_text.txt'.format(today_date), 'a+') as f:
        for i in one_info:
            if isinstance(i, dt.date):
                i = i.strftime("%Y-%y-%m")
            if isinstance(i, list):
                i = [j.strftime("%Y-%y-%m") if isinstance(j, dt.date) else j for j in i]
                i = ','.join(i)
            f.write(i)
            f.write('\t')
        f.write('\n')
        f.close()
    shutil.rmtree(base_dir)


def request_accounts(camps_list):
    global today_date, zip_dir
    today_date = datetime.now().date().strftime("%Y-%m-%d")
    with open(r'C:\Users\Administrator\Desktop\{}_new_api_request_part_text.txt'.format(today_date), 'a+') as f:
        f.write('account\tsite\trequest_date\tcamp_dates\tbusiness_dates\tsearch_dates\n')
        f.close()
    logging.basicConfig(filename="new_api_request.log", filemode="a+", format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                        datefmt="%Y-%M-%d %H:%M:%S", level=logging.ERROR)
    zip_dir = 'C:/KMSOFT/Download/station_report1/'
    if not os.path.exists(zip_dir):
        os.mkdir(zip_dir)
    all_threads = []
    ta = datetime.now()
    for camp in camps_list:
        # process_one_camp(camp)
        one_thread = threading.Thread(target=process_one_camp, args=(camp,))
        all_threads.append(one_thread)
    for my_thread in all_threads:
        my_thread.start()
    for my_thread in all_threads:
        my_thread.join()
    tb = datetime.now()
    print("请求以及读取内容写入内容一共花费: {}秒".format(tb-ta))


def request_url(**kwargs):
    file = kwargs['file_name']
    try:
        file_r = requests.get(file)
    except:
       # print("请求不成功!!!", file_r.status_code)
        file_r = []
        try_times = 1
        while 1:
            try_times += 1
            print("第{}次请求{}...".format(try_times, file))
            try:
                file_r = requests.get(file)
                break
            except:
                time.sleep(2)
            if try_times > 3:
                logging.exception(
                    "\tTYPE1.1: CANOT REQUEST: {}_{}  DAY: {}".format(kwargs["account"], kwargs['site'], file[-43:-33]))
                break
    if not file_r:
        return
    file_name = file.split('/')[-1]
    status_code = file_r.status_code
    if status_code == 200:
        out_content = file_r.content  # {'data': file_r.content, 'msg': 'complete'}
    else:
        # print(status_code)
        return
        # {'data': "", 'msg': status_code}
    one_base_dir = zip_dir + kwargs['account'] + "_" + kwargs['site'] + '_{}'.format(today_date)
    with open(one_base_dir + '/' + file_name, 'wb') as f:
        f.write(out_content)


# 将请求的站点写入到临时文件中
def write_files(**kwargs):
    download_url = "http://120.78.243.154//services/advertising/reportdownload/{}".format(kwargs['type'])
    if kwargs['type'] in ['campaign', 'search']:
        post_load = {
            'site': kwargs['site'],
            'account': kwargs['account'],
            'report_start_date': kwargs['report_start_date'],
            'report_end_date': kwargs['report_end_date'],
        }
    else:
        post_load = {
            'site': kwargs['site'],
            'account': kwargs['account'],
        }
    response = requests.post(download_url, data=post_load).content
    data_url = response.decode()
    if json.loads(data_url)['code'] == 1000:
        json_data = json.loads(data_url)['data']
        file_list = [camp_file['file'] for camp_file in json_data]
        return file_list
    else:
        file_list = []
        return file_list
    # file_list = unzip_dir(base_dir)


# 提取文件名中的日期
def parse_date(file_name):
    file_date = re.findall('2019.[0-9]{2}.[0-9]{2}', file_name)
    file_date = file_date[-1]
    if len(file_date) == 10:
        file_date = file_date[0:4] + '-' + file_date[5:7] + '-' + file_date[-2:]
    if len(file_date) == 8:
        file_date = file_date[0:4] + '-' + file_date[4:6] + '-' + file_date[-2:]
    return file_date


# 找到所有请求类型的表日期
def find_report_dates(**kwargs):
    a_base_dir = zip_dir + kwargs['account'] + "_" + kwargs['site'] + '_{}'.format(today_date)
    files_name = os.listdir(a_base_dir)
    report_list = [file_name for file_name in files_name if file_name.find("{}".format(kwargs['keyword'])) > 0]
    report_date = sorted([parse_date(file_name) for file_name in report_list])
    return report_date


# 读取临时文件下站点的日期
def read_reports(**kwargs):
    a_account, a_site = kwargs['account'], kwargs['site']
    camp_dates, business_dates, search_dates = find_report_dates(keyword='Bulk', account=a_account, site=a_site), find_report_dates(
        keyword='Business', account=a_account, site=a_site), find_report_dates(keyword='Search', account=a_account, site=a_site)
    all_info = [kwargs['account'], kwargs['site'], today_date, camp_dates, business_dates, search_dates]
    return all_info


# 读取站点信息
def read_account(path):
    account_info = pd.read_excel(path, sheet_name=0)
    account_name = account_info['account_num']
    new_accounts = []
    for i, j in zip(account_name, account_info['site']):
        # if j == 'sp' or j == 'SP':
        #     j = 'es'
        new_account = i + '_' + j
        new_accounts.append(new_account)
    return new_accounts


def walk_account():
    files_path = 'C:/Users/Administrator/Desktop/yibai_amazon_account2.xlsx'
    all_camps_name = read_account(files_path)
    request_accounts = all_camps_name[600:610]
    return request_accounts


# 主循环，遍历
if __name__ == '__main__':
    t0 = datetime.now()
    accounts = walk_account()
    print(accounts)
    request_accounts(accounts)
    t1 = datetime.now()
    print("一共花费: {} 秒".format((t1 - t0).seconds))



