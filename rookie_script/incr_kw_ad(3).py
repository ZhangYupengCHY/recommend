#!/usr/bin/env python
# coding=utf-8
# author:marmot

import pandas as pd
import numpy as np
import tkinter as tk
import win32api
from tkinter import filedialog
import datetime
import time
from tkinter import ttk
import re
import os
# import sys



# 导入sku和kw数据表
def sku_kws_data(file_path):
    data = pd.read_excel(file_path)
    data.columns = [column.upper() for column in list(data.columns)]
    data = data[['SKU', 'KEYWORD']]
    return data


# 将sku和kw数据表分组
def group_sku_and_kws(data):
    global sep_word
    sep_word = 'sep'
    data.fillna(sep_word, inplace=True)
    sep = [0]
    for i in range(1, data.shape[0] - 1):
        if (data.ix[i, 'SKU'] == sep_word) & (data.ix[i + 1, 'SKU'] != sep_word):
            sep.append(i)
    last_sku = max(len(data['SKU']),len(data['KEYWORD']))
    my_dict = {}
    len_sep = len(sep)
    if len_sep > 1:
        my_dict[0] = data.ix[sep[0]:sep[1] - 1, :]
        for i in range(2, len_sep):
            my_dict[i] = data.ix[sep[i - 1] + 1:sep[i] - 1, :]
        my_dict[len_sep] = data.ix[sep[-1] + 1:last_sku+1, :]
        print('c')
    else:
        my_dict[0] = data
    return my_dict


# 计算每个店铺的每天的预算
def camp_daily_budget(all_info, site, one_sku_budget):
    len_sku = all_info['SKU'][all_info['SKU'].notnull()].count()
    # print(one_sku_budget)
    camp_budget = round(min(len_sku * one_sku_budget, 20) / change_current[site], 2)
    return camp_budget


# 计算sku与kw
def calc_sku_and_kws(base_dir, account, site, camp_name, ad_group_name, ad_manger, one_sku_budget, one_kw_price):
    global change_current, my_columns, campaign_name_info
    # 表的列名
    my_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                  'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword', 'Match Type',
                  'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']
    # 汇率
    change_current = {'US': 1, 'CA': 1, 'UK': 1, 'DE': 1, 'IT': 1,
                      'ES': 1, 'JP': 0.008997, 'FR': 1, 'MX': 0.0513,
                      'IN': 0.01418, 'AU': 1}
    # sku以及kws信息
    sku_info = sku_kws_data(base_dir)
    # 计算店铺的预算
    camp_budget = camp_daily_budget(sku_info, site, one_sku_budget)
    # 所有的信息
    empty_list = [np.nan] * len(my_columns)
    first_info = pd.DataFrame([empty_list], columns=my_columns)
    if camp_name in ['', '自定义广告大组的名字']:
        campaign_name_info = "MANUAL-" + account + "_" + site + "-MANY_SKU_KW"
    else:
        campaign_name_info = camp_name  # 默认的广告大组名字为MANUAL- + account + -station + -SKU_KW
    # 初始化第一行
    first_info.ix[0, 'Campaign Targeting Type'] = 'MANUAL'
    first_info.ix[0, 'Campaign Daily Budget'] = camp_budget
    first_info.ix[0, 'Campaign Status'] = 'enabled'
    # 将sku分组分开（按导入的excel文件中的空格分组）
    grouped_sku = group_sku_and_kws(sku_info)
    all_rest_info = pd.DataFrame(columns=my_columns)
    for value in grouped_sku.values():
        if value.empty:
            continue
        # ad_group 第一行数据（第一部分）
        empty_list = [np.nan] * len(my_columns)
        first_row = pd.DataFrame([empty_list], columns=my_columns)
        first_row['Max Bid'] = round(0.02 / change_current[site], 2)
        first_row['Ad Group Status'] = 'enabled'
        # ad_group 第二部分数据 sku
        second_part = pd.DataFrame(columns=my_columns)
        second_part['SKU'] = list(set(value['SKU']))
        second_part['Status'] = 'enabled'
        # ad_group 第三部分  出价和kw
        third_part = pd.DataFrame(columns=my_columns)
        third_part['Keyword'] = value['KEYWORD'][value['KEYWORD'] != sep_word]
        third_part['Keyword'] = third_part['Keyword'].apply(lambda x: x.replace(' '*2, ' ').replace(' '*3, ' ')).replace(' ',' ')
        third_part.reset_index(drop=True, inplace=True)
        third_part['Keyword'] = third_part['Keyword'].apply(lambda x:' '.join(x.split(' ')))
        third_part['Max Bid'] = third_part['Keyword'].apply(
            lambda x: round(min(len(x.split(' ')) * one_kw_price, 0.25) / change_current[site], 2))
        third_part['Match Type'] = "phrase"
        third_part['Status'] = 'enabled'
        rest1_info = pd.concat([first_row, second_part, third_part], sort=False)
        rest1_info.drop_duplicates(inplace=True)
        all_rest_info = pd.concat([all_rest_info, rest1_info], sort=False)


    # 添加广告组的名字
    all_rest_info.fillna('', inplace=True)
    all_rest_info.reset_index(drop=True, inplace=True)
    sku_list = all_rest_info['SKU']
    flag, now_time_str = 0, str(int(time.time()))
    adgroup_index = [one_index for one_index in range(len(sku_list))
                     if sku_list[one_index] == "" and
                     one_index + 1 < len(sku_list) and
                     sku_list[one_index + 1] != ""] + [all_rest_info.index[-1]+1]
    # print adgroup_index
    for one_index in range(len(adgroup_index)):
        flag += 1
        first_index = adgroup_index[one_index]
        if one_index + 1 < len(adgroup_index):
            last_index = adgroup_index[one_index + 1]
        else:
            last_index = adgroup_index[-1]
        if ad_group_name in ['', '自定义广告组的名字']:
            all_rest_info.loc[first_index:last_index, 'Ad Group Name'] = \
                "MANUAL_KW_Group-" + str(flag) + "-" + now_time_str
        else:
            all_rest_info.loc[first_index:last_index, 'Ad Group Name'] = \
                ad_group_name + str(flag) + "-" + now_time_str  # ad_group_name默认名字为KW_MANUAL
    # 加上预算行
    all_info = pd.concat([first_info, all_rest_info], sort=False)
    # 添加广告大组的名字
    all_info['Campaign Name'] = campaign_name_info

    # 规范成标准列
    all_info = all_info[my_columns]

    # 删除列中的sep
    all_info = all_info[all_info['SKU'] != sep_word]

    # 生辰输出文件夹
    now_date = datetime.datetime.now().strftime("%Y%m%d")
    now_dir = os.path.dirname(base_dir)
    report_dir = now_dir + "/report"
    if not os.path.exists(report_dir):
        os.mkdir(report_dir)
    out_path = report_dir + "/" + now_date + " 一组多SKU关键词广告.xlsx"
    all_info.to_excel(out_path, na_rep='', index=False)

    return [account, site, camp_budget, report_dir]


def screen_size(root, top, ww, wh):
    x = (root.winfo_width() - ww) / 2
    y = (root.winfo_height() - wh) / 2
    root.update()
    right = root.winfo_x() + x
    down = root.winfo_y() + y
    top.geometry("%dx%d+%d+%d" % (ww, wh, right, down))


def base_screen_size(root, ww, wh, right, down):
    root.geometry("%dx%d+%d+%d" % (ww, wh, right, down))


def right_screen_size(root, top, ww, wh):
    x = root.winfo_width()
    root.update()
    right = root.winfo_x() + x + 16
    down = root.winfo_y() + 5
    top.geometry("%dx%d+%d+%d" % (ww, wh, right, down))


def keyword_windows(root):
    def select_file():
        init_dir = r'D:\待处理'
        file1 = filedialog.askopenfilename(initialdir=init_dir, parent=root)
        if re.search(r'[A-Za-z\\]', file1):
            path1.set(file1)
            try:
                # shop_station = get_shop_station_from_folder.get_store_station_file(file1)
                shop_station = os.path.basename(os.path.dirname(file1))
                store = shop_station[0:-3]
                station_abbr = shop_station[-2:]
                shopname.set(store)
                station.set(station_abbr)
                if station_abbr == 'JP':
                    budget.set(100)
                    per_kw_price.set(2)
                else:
                    budget.set(2)
            except:
                pass

    def getcontent(anyvar):
        content = anyvar.get()  # 获取文本框内容
        return content

    def analysis():
        if getcontent(path1) == ' ':
            # 创建一个顶级弹窗
            top = tk.Toplevel()
            top.attributes("-toolwindow", 1)
            top.wm_attributes("-topmost", 1)
            screen_size(root, top, 100, 30)
            msg = tk.Message(top, text='未选择文件', width=100)
            msg.pack()
        else:
            # calc_sku_and_kws(test_path, 'kimiss', 'UK', 'manual_kimiss_sp', 'MANUAL_KWS', 'WY', 1, 0.05)
            all_list = calc_sku_and_kws(getcontent(path1), getcontent(shopname), getcontent(station),
                                        getcontent(camp_name), getcontent(adgroup_name), '', float(getcontent(budget)),
                                        float(getcontent(per_kw_price)))
            file_path1 = all_list[3]
            win32api.ShellExecute(0, 'open', file_path1, ' ', ' ', 1)

    root.resizable(False, False)
    root.title('一组多SKU-MANUAL广告')
    root.attributes("-toolwindow", 1)

    # 创建一个下拉列表
    shopname = tk.StringVar()
    numberChosen1 = ttk.Combobox(root, width=20, textvariable=shopname)
    numberChosen1['values'] = ('选择店铺名', 'HUHUSHOP',)  # 设置下拉列表的值
    numberChosen1.grid(row=0, column=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
    numberChosen1.current(0)

    # 创建一个下拉列表
    station = tk.StringVar()
    numberChosen2 = ttk.Combobox(root, width=20, textvariable=station)
    numberChosen2['values'] = ('请选择站点', 'Amazon.com', 'Amazon.ca',
                               'Amazon.mx', 'Amazon.fr', 'Amazon.co.uk',
                               'Amazon.de', 'Amazon.es', 'Amazon.it',
                               'Amazon.jp')  # 设置下拉列表的值
    numberChosen2.grid(row=1, column=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
    numberChosen2.current(0)

    # 显示广告大组名字
    camp_name = tk.StringVar()
    camp_name.set('自定义广告大组的名字')
    # path1.set(r'C:/Users/Administrator/Desktop/zjchao-jp.xlsx')
    tk.Entry(root, width=22, fg='red', textvariable=camp_name).grid(row=2, column=0)

    # 显示广告组名字
    adgroup_name = tk.StringVar()
    adgroup_name.set('自定义广告组的名字')
    # path1.set(r'C:/Users/Administrator/Desktop/zjchao-jp.xlsx')
    tk.Entry(root, width=22, fg='red', textvariable=adgroup_name).grid(row=3, column=0)

    # 单个sku预算
    budget = tk.StringVar()
    numberChosen = ttk.Combobox(root, width=20, textvariable=budget)
    numberChosen['values'] = ('填写预算系数', '2', '5', '100')  # 设置下拉列表的值
    numberChosen.grid(row=4, column=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
    numberChosen.current(0)

    # 单个关键词出价
    per_kw_price = tk.StringVar()
    per_kw_price.set(0.05)
    tk.Entry(root, width=22, textvariable=per_kw_price).grid(row=5, column=0)

    # 显示文件路径
    path1 = tk.StringVar()
    path1.set(' ')
    # path1.set(r'C:/Users/Administrator/Desktop/zjchao-jp.xlsx')
    tk.Entry(root, width=22, textvariable=path1).grid(row=6, column=0)

    tk.Button(root, text="文件选择", command=select_file).grid(row=7, column=0)

    tk.Button(root, text="点击按钮开始生成", command=analysis).grid(row=8, column=0)  # command绑定获取文本框内容方法


if __name__ == "__main__":
    # test_path = r'C:\Users\Administrator\Desktop\CAREDY_US\CAREDY_US-KW-2019-10-19 17-05-28.xlsx'
    # out_path = r'C:\Users\Administrator\Desktop\out.xlsx'
    # calc_sku_and_kws(test_path,'kimiss', 'UK', 'manual_kimiss_sp','MANUAL_KWS','WY',1,0.05, out_path)
    root1 = tk.Tk()
    base_screen_size(root1, 165, 215, 350, 180)
    keyword_windows(root1)
    root1.mainloop()

print(' '.join('xyz linear stage'.split(' ')))