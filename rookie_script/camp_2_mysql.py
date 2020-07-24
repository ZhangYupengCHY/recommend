# -*- coding: utf-8 -*-
"""
Created on Tue Aug 20 15:23:41 2019

@author: Administrator
"""

import requests
import win32api
import os
import zipfile
from datetime import datetime
import numpy as np
import pandas as pd
import shutil
import re
import time
import pymysql
import traceback
import sys
sys.path.append('D:\\AD-Helper1\\ad_helper\\init_proc')
from read_campaign import read_campaign
import init_campaign,translation



# =============================================================================
# 可能出现的问题：
#    1.
#     不同的表格会有新的列：columns = ['Record ID', 'Record Type', 'Campaign', 'Campaign Daily Budget', 'Portfolio ID', 'Campaign Start Date',
#                 'Campaign End Date', 'Campaign Targeting Type', 'Ad Group', 'Max Bid',
#                 'Keyword or Product Targeting', 'Product Targeting ID', 'Match Type',
#                 'SKU', 'Campaign Status', 'Ad Group Status', 'Status', 'Impressions',
#                 'Clicks', 'Spend', 'Orders', 'Total Units', 'Sales', 'ACoS']
#   2.
#      表数据重复写入到数据库
# =============================================================================


# 将dataframe写入到mysql中
def write_to_mysql(df):
    df = df.astype(object).where(pd.notnull(df), None)
    df = np.array(df)
    # 创建连接
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='camp_report',
        port=3306,
        charset='UTF8')

    all_list = []
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)
    # df.to_sql()
    # 写入到数据库中
    # 创建游标
    cursor = conn.cursor()
    sql = """insert into sku_report_test2 (Record_ID, Record_Type, Campaign, Campaign_Daily_Budget, Portfolio_ID,\
    Campaign_Start_Date,Campaign_End_Date, Campaign_Targeting_Type, Ad_Group, Max_Bid,Keyword_or_Product_Targeting, \
    Product_Targeting_ID,Match_Type, SKU, Campaign_Status, Ad_Group_Status,Status, Impressions, Clicks, Spend, Orders, \
    Total_Units, Sales, ACoS,date) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
      %s,%s,%s,%s,%s,%s,%s,%s)"""
    try:
        cursor.executemany(sql, all_list)
    except Exception as e:
        print(e)
    conn.commit()
    cursor.close()
    conn.close()



# 查找某个站点的bulks文件下的所有数据
def get_bulk(files_list):
    global columns_name
    print(base_dir)
    files = [filename for filename in files_list if filename.find('Bulk') >= 0]
    if len(files) > 0:
        camp_all_date_info = pd.DataFrame([])
        for file in files:
            camp_date = parse_date(file)
            path = base_dir + '/' + file
            try:
                bulk_data = read_campaign(path)
                station_country = station_country.upper()
                if station_country == 'SP':
                   station_country = 'ES'
                else:
                    pass
                bulk_data = init_campaign(bulk_data,station_country,'empty')
                print('正在读取{}'.format(path))
                print(bulk_data.shape)
# =============================================================================
#                 bulk_data = pd.ExcelFile(path)
#                 sheet_name = [i for i in bulk_data.sheet_names if i.find("Products") > 0][0]
#                 bulk_data = bulk_data.parse(sheet_name)
#                 bulk_data = bulk_data.iloc[:, 0:24]
#                 print('正在读取{}'.format(path))
#                 print(bulk_data.shape)
# =============================================================================
            except:
                bulk_data = pd.DataFrame([])
                print("{}不存在数据".format(path))
            if bulk_data.shape[0] == 0:
                pass
            else:
                all_new_columns_name = []
                for column_name in bulk_data.columns:
                    new_column_name = column_name.replace(' ',"_")
                    all_new_columns_name.append(new_column_name)
                for old_column_name,new_column_name in zip(bulk_data.columns,all_new_columns_name):
                    bulk_data.rename(columns={old_column_name:new_column_name},inplace = True)
                columns_name = ['Record_ID', 'Record_Type', 'Campaign', 'Campaign_Daily_Budget', 'Portfolio_ID', "\
           " 'Campaign_Start_Date', 'Campaign_End_Date', 'Campaign_Targeting_Type', 'Ad_Group', 'Max_Bid', "\
           " 'Keyword_or_Product_Targeting', 'Product_Targeting_ID', 'Match_Type', 'SKU', 'Campaign_Status', "\
           " 'Ad_Group_Status','Status', 'Impressions', 'Clicks', 'Spend', 'Orders', 'Total_Units', 'Sales', 'ACoS']
                bulk_data = bulk_data[columns_name]
                bulk_data['date'] = camp_date
                print("正在写入{},请耐心等待...".format(file))
                try:
                    write_to_mysql(bulk_data)
                    print("写入完毕...")
                except Exception as e:
                    print(e)
                    traceback.print_exc()
            camp_all_date_info = pd.concat([camp_all_date_info, bulk_data], axis=0, sort=False, ignore_index=True)
            print(path, camp_all_date_info.shape[0])
    else:
        camp_all_date_info = pd.DataFrame()
    return camp_all_date_info


# 解压文件包
def unzip_dir(zip_dir):
    base_dir = zip_dir.split('.')[0]
    if not os.path.exists(base_dir):
        os.mkdir(base_dir)
    z = zipfile.ZipFile(zip_dir, "r")
    # 打印zip文件中的文件列表
    file_name_list = z.namelist()
    for filename in file_name_list:
        # 读取zip文件中的第一个文件
        content = z.read(filename)
        with open(base_dir + '/' + filename, 'wb') as f:
            f.write(content)
    return file_name_list


# 获得active,business,search这三个表的信息
def find_str(files_name):
    tmp_series = pd.Series([])
    check_reports = ['active', 'business', 'search']
    for report in check_reports:
        if (np.array([filename.find(report) for filename in files_name]) >= 0).any():
            tmp_series[report] = '完整'
        else:
            tmp_series[report] = '不完整'
    return tmp_series


# 获得日期字符串
def parse_date(file_name):
    file_date = re.findall('2019.[0-9]{2}.[0-9]{2}', file_name)
    file_date = file_date[0]
    return file_date


# 将时间的字符串格式转换成时间的集合
def parse_list_2_date(str_list):
    if len(str_list) > 0:
        sign_interval = str_list[0][4]
        date_list = [datetime.strptime(i, '%Y{}%m{}%d'.format(sign_interval, sign_interval)) for i in str_list]
    else:
        date_list = []
    return date_list


# 计算日期差
def diff_date():
    # camp表是提前两天，order是提前一天
    date_diff = (datetime.now() - datetime(2019, 8, 7)).days
    return date_diff


# 读取所有站点,同时处理无法请求的问题
def request_camps(camps_list):
    all_info = pd.DataFrame()
    for camp in camps_list:
        try:
            one_info = request_ad(camp)
            print("请求成功！！！")
        except:
            times = 0
            print("Oops,第{}次请求失败，现在是{}次请求".format(times + 1, times + 2))
            while True:
                times += 1
                time.sleep(10)
                try:
                    one_info = request_ad(camp)
                    break
                except:
                    one_info = pd.DataFrame()
                if times > 3:
                    break
        all_info = pd.concat([all_info, one_info], axis=0, sort=False, ignore_index=True)
    #        print("目前正运行到第{}个,请耐心等待,还剩下{}个.".format(100*m+j,(len_camps-100*m-j)))
    return all_info


# 读取一各站点下的所有文件
def request_ad(camp):
    global base_dir
    download_url = "http://192.168.129.240:8080/ad_api/download"
    post_load = {'shop_station': '{}'.format(camp), 'passport': 'marmot'}
    response = requests.post(download_url, data=post_load).content
    data_url = response.decode()
    if 'http' not in data_url:
        return pd.DataFrame()
    # print(data_url)
    file_r = requests.get(response)
    status_code = file_r.status_code
    if status_code == 200:
        out_content = file_r.content  # {'data': file_r.content, 'msg': 'complete'}
    else:
        return pd.DataFrame()  # {'data': "", 'msg': status_code}
    zip_dir = 'C:/KMSOFT/Download/station_report/'
    if not os.path.exists(zip_dir):
        os.mkdir(zip_dir)
    now_date = datetime.now().date().strftime('%Y%m%d')
    base_dir = zip_dir + '{}'.format(camp).upper() + '_{}'.format(now_date)
    # print(base_dir)
    station_zip = base_dir + '.zip'
    with open(station_zip, 'wb') as f:
        f.write(out_content)
    files_list = unzip_dir(station_zip)
    print(files_list)
    one_camp_all_days_info = get_bulk(files_list)
    print("one_camp_all_days_info:{}".format(one_camp_all_days_info.shape))
    #     zhandian_info = get_reports(file_list)
    shutil.rmtree(base_dir)
    # win32api.ShellExecute(0, 'open',zip_dir, ' ', ' ', 0)
    os.remove(station_zip)
    return one_camp_all_days_info


# 读取站点信息
def read_account(path):
    global station_country
    account_info = pd.read_excel(path, sheet_name=0)
    account_name = [''.join(re.findall('[a-zA-Z]', accont)) for accont in account_info['account_name']]
    new_accounts = []
    for i, j in zip(account_name, account_info['site']):
        station_country = j
        new_account = i + '_' + j
        new_accounts.append(new_account)
    return new_accounts


if __name__ == '__main__':
    t0 = datetime.now()
    print('程序开始于:{}'.format(t0))
    today_date = t0.date()
    path = r'C:\Users\Administrator\Desktop\yibai_amazon_account.xlsx'
    all_camps_name = read_account(path)
    request_camps_name = all_camps_name[0:2]
    result_info = request_camps(request_camps_name)
    t1 = datetime.now()
    result_info.to_csv(r'C:\Users\Administrator\Desktop\12345.txt', index=False, sep='\t')
    print('程序结束于:{}'.format(t1))
    print('程序运行: {}s'.format((t1 - t0).seconds))



