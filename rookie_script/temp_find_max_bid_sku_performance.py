# -*- coding: utf-8 -*-
"""
Created on Wed Sep  4 18:34:47 2019

@author: Administrator
"""

import pandas as pd
import os

path = 'C:/KMSOFT/Download/station_report/NITRIP_UK_20190905'
files = os.listdir(path)

df = pd.DataFrame()
for file in files:
    file_path = path + '/' + file
    file_data = pd.read_excel(file_path, sheet_name=1)
    file_data['date'] = file[-43:-33]
    df = pd.concat([df, file_data], ignore_index=True, sort=False)

max_sku_order_index = df['Orders'][df['Record Type'] == 'Ad'].ixdmax()
print("最大订单{}".format(max(df['Orders'][df['Record Type'] == 'Ad'])))
ad_group_name = df.ix[max_sku_order_index, 'Ad Group']
max_bid = df['Max_Bid'][(df['Record Type'] == 'Ad Group') & (df['Ad Group'] == ad_group_name)]