# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/4/22 16:37
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
import time
from datetime import datetime
import re

# 读取单个文件数据(若为excel,则读取单个sheet)
def read_files(files_path: 'full_path', sheet_name='Sheet1'):
    split_file_path = os.path.splitext(files_path)
    if len(split_file_path) > 1:
        file_type = split_file_path[-1].lower()
        if file_type in ['.csv', '.txt']:
            try:
                file_data = pd.read_csv(files_path, error_bad_lines=False, warn_bad_lines=False)
                if file_data.shape[1] == 1:
                    file_data = pd.read_csv(files_path, sep='\t',error_bad_lines=False, warn_bad_lines=False)
                return file_data
            except Exception as e:
                file_data = pd.read_csv(files_path, encoding="ISO-8859-1", error_bad_lines=False, warn_bad_lines=False)
                if file_data.shape[1] == 1:
                    file_data = pd.read_csv(files_path, sep='\t', encoding="ISO-8859-1",error_bad_lines=False, warn_bad_lines=False)
                return file_data
            except Exception as e:
                print(f"文件无法被正确读取: {files_path}")
        if file_type in ['.xlsx', '.xls']:
            try:
                if sheet_name == 'Sheet1':
                    file_data = pd.read_excel(files_path)
                    return file_data
                else:
                    read_excel = pd.ExcelFile(files_path)
                    sheet_names = read_excel.sheet_names
                    if sheet_name not in sheet_names:
                        print(f'{files_path}中没有{sheet_name}.')
                        return
                    else:
                        file_data = read_excel.parse(sheet_name)
                        return file_data
            except Exception as e:
                print(f"文件无法被正确读取:{files_path}")
        else:
            print(f'文件不为文本格式,请检查文件:{files_path}')
    else:
        print(f'请检查文件是否为有效路径:{files_path}')


sheet_dict = {'出单优质搜索词':
                  {'sql_table_name': 'high_quality_kws'},
              '未出单高点击搜索词':
                  {'sql_table_name': 'high_click_no_order_kws'},
              '近期低于平均点击率的SKU':
                  {'sql_table_name': 'lower_ctr_sku'},
              '后台Search Term参考':
                  {'sql_table_name': 'st_refer'},
              '不出单关键词':
                  {'sql_table_name': 'no_order_kws'}}

print(sheet_dict['出单优质搜索词']['sql_table_name'])