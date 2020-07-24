# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/12/20 11:58
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

# !/usr/bin/env python
# coding=utf-8
# author:marmot

import rsa, json, requests, os, redis, zipfile, shutil, time, re
import pandas as pd
from retry import  retry
# import Crypto.PublicKey.RSA
import base64, pymysql
from datetime import datetime
import xlsxwriter

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor



a=[1,2,3]
print(a.items)



#
#
#
#
#
# THREAD_POOL = ThreadPoolExecutor(6)
# PROCESS_POOL = ProcessPoolExecutor(2)
# redis_pool_6 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=6, password='chy910624', decode_responses=True)
# redis_pool_7 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=7, password='chy910624', decode_responses=True)
# red = redis.StrictRedis(connection_pool=redis_pool_7)
# red_station_status = redis.StrictRedis(connection_pool=redis_pool_6)
#
# not_complete_stations = red_station_status.keys()
# complete_station = red.lrange('complete_files_station', 0, -1)
# file_sign_dict = {'camp_1': '成功推送广告报表1天', 'camp_7': '成功推送广告报表7天', 'camp_14': '成功推送广告报表14天', 'camp_30': '成功推送广告报表30天',
#                   'camp_60': '成功推送广告报表60天', 'active_listing': '成功推送active_listing报表',
#                   'all_listing': '成功推送all_listing报表', 'all_orders': '成功推送订单报表',
#                   'business': '成功推送业务报表', 'search_term_this_month': '搜索词当月1号至今', 'search_term_last_month': '搜索词上月数据'}
#
# now_date = datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
# save_log_path = f'D:/api_request_all_files/request_log_{now_date}.xlsx'
# # 创建一个workbook 设置编码
# workbook = xlsxwriter.Workbook(save_log_path)
# # 创建一个worksheet
# worksheet = workbook.add_worksheet('站点请求结果')
#
# # 写入excel
# # 参数对应 行, 列, 值
# worksheet.write(0, 0, '时间')
# worksheet.write(1, 0, f'{now_date}')
# worksheet.write(0, 1, '推送过来完整数据账号')
# worksheet.write(1, 1, f'{len(complete_station)}')
#
# stations_lost_files = {station: red_station_status.get(station).split(',') for station in not_complete_stations}
# num = 2
# for file_type_sign_word, file_type_name in file_sign_dict.items():
#     file_type_success_stations = [station for station in not_complete_stations if
#                                   file_type_sign_word not in stations_lost_files[station]]
#     file_type_success_stations.extend(complete_station)
#     type_len = len(file_type_success_stations)
#     worksheet.write(0, num, f'{file_type_name}')
#     worksheet.write(1, num, f'{type_len}')
#     i = 2
#     for station in file_type_success_stations:
#         worksheet.write(i, num, station)
#         i += 1
#     num += 1
#
# workbook.close()
#
# # path = r"D:\api_request_all_files\2020-03-01\acogedor_in\acogedor-in-1天-bulksheet-03-01-2020.xlsx"
# # data= pd.read_excel(path,sheet_name='Sponsored Products Campaigns')
# # ad_data = data[data['Record Type']=='Ad']
# # empty = ad_data[pd.isna(ad_data['SKU'])]
