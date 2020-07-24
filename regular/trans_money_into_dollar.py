# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/3/9 14:57
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import pandas as pd
import numpy as np
import os,pymysql
import re
import difflib
from datetime import datetime
import re
import shutil


import os
import time
import pandas as pd
import my_toolkit.process_files as process_files

sale_exchange_rate = {'CA': 0.7519, 'DE': 1.0981, 'FR': 1.0981, 'IT': 1.0981, 'SP': 1.0981, 'JP': 0.009302,
                      'UK': 1.2445, 'MX': 0.05147, 'IN': 0.01412, 'US': 1, 'ES': 1.0981, 'AU': 0.6766}


def trans_local_money_into_dollar(ori_data):
    """
    将only_station_info中的本币按照汇率换算成美金
    :param file_path:
    :return:
    """
    ori_data['site'] = ori_data['station'].apply(lambda x: x[-2:].upper())
    for column in ['ad_sales', 'shop_sales', 'cpc']:
        ori_data[column] = [round(column * sale_exchange_rate[site], 2) for column, site in
                            zip(ori_data[column], ori_data['site'])]
    print(ori_data)
    new_path = os.path.splitext(file_path)[0]+'_new' + os.path.splitext(file_path)[1]
    ori_data.to_excel(new_path,index=False)
    print('oky')

if __name__ == "__main__":
    file_path = r"C:\Users\Administrator\Desktop\查询站点.txt"
    file_data = process_files.read_file(file_path)
    trans_local_money_into_dollar(file_data)
