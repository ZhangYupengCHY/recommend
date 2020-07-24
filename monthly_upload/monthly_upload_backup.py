# -*- coding: utf-8 -*-
"""
Proj: recommend
Created on:   2020/1/8 10:56
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
import glob
import pandas as pd
import re
import tkinter as tk
import tkinter.messagebox
from tkinter import ttk
import traceback
from datetime import datetime
import pymysql

# import numpy as np
# import warnings

##############################################################GUI##############################################################################

# warnings.filterwarnings("ignore")
# 汇率
exchange_rate = {'CA': 0.7686, 'DE': 1.113, 'FR': 1.113, 'IT': 1.113, 'SP': 1.113, 'JP': 0.009302,
                 'UK': 1.3116, 'MX': 0.05302, 'IN': 0.01394, 'US': 1, 'ES': 1.113, 'AU': 0.6766}

win = tk.Tk()
win.title("Monthly Combine")  # 添加标题

ttk.Label(win, text="Choose Your Name:").grid(column=0, row=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
ttk.Label(win, text="Folder Address:").grid(column=0, row=1)  # 添加一个标签，并将其列设置为1，行设置为0

# Name 下拉列表
person = tk.StringVar()
personChosen = ttk.Combobox(win, width=20, textvariable=person)
personChosen['values'] = ("请选择姓名",
                          "陈梦文", "陈实", "但蕾", "何莲",
                          "李晨", "李萌", "李娜", '马良', '贾晨阳', '陈悦', '柯凡', '李舜禹',
                          "廖凯锋", "刘晓颖", "刘银萍", "马良", "毛汉芬", '曾德霞', '虢智蕊',
                          "毛洵", "彭舒婷", "苏雅丽", "w2/.汪维", '刘艳', '刘碗菊', '羌子君',
                          "王艳", "向江燕", "晏光宇", "张立滨", '赵天钰', '周丹', '崔传周', '时丹丹', '廖丹', '丁贝'
                          , "待加入姓名")  # 设置下拉列表的值
personChosen.grid(column=1, row=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
personChosen.current(0)  # 设置下拉列表默认显示的值，0为 numberChosen['values'] 的下标值

# Address 文本框
address = tk.StringVar()  # StringVar是Tk库内部定义的字符串变量类型，在这里用于管理部件上面的字符；不过一般用在按钮button上。改变StringVar，按钮上的文字也随之改变。
addressEntered = ttk.Entry(win, width=40,
                           textvariable=address)  # 创建一个文本框，定义长度为12个字符长度，并且将文本框中的内容绑定到上一句定义的name变量上，方便clickMe调用
addressEntered.grid(column=1, row=1)  # 设置其在界面中出现的位置  column代表列   row 代表行
addressEntered.focus()  # 当程序运行时,光标默认会出现在该文本框中

# 提示信息 Text
tip = tk.Label(win, background='seashell', foreground='red',
               text='地址类似于' + r'D:\工作\2020年2月' + ', 只能包含一个\'年\'一个\'月\'')
tip.grid(column=1, row=2)


##############################################################Functions#################################################################################
# button被点击之后会被执行

# 初始化advertise_product表数据
def init_ad_product(ad_product: pd.DataFrame) -> pd.DataFrame:
    """
    初始化advertise product中的表数据:主要是货币单位的列
    :param ad_product: advertise product数据
    :return: 初始化后的advertise product
    """
    columns_name = [col.replace('($)', '').replace('(£)', '').replace('(￥)', '').replace('(₹)','').strip(" ") for col in
                    ad_product.columns]
    site = ad_product['country'].values[0].upper()
    ad_product.columns = columns_name
    ad_product.rename(
        columns={'Advertising Cost of Sales (ACoS)': 'Total Advertising Cost of Sales (ACoS)', '开始日期': 'Start Date',
                 '结束日期': 'End Date', '广告组合名称': 'Portfolio name', '货币': 'Currency', '广告活动名称': 'Campaign Name',
                 '广告组名称': 'Ad Group Name', '广告SKU': 'Advertised SKU', '广告ASIN': 'Advertised ASIN', '展现量': 'Impressions',
                 '点击量': 'Clicks', '点击率(CTR)': 'Click-Thru Rate (CTR)', '每次点击成本(CPC)': 'Cost Per Click (CPC)',
                 '花费': 'Spend', '7天总销售额': '7 Day Total Sales',
                 '广告成本销售比(ACoS)': 'Total Advertising Cost of Sales (ACoS)', '7天总订单数(#)': '7 Day Total Orders (#)'},
        inplace=True)
    init_cols = ['Cost Per Click (CPC)', 'Spend', '7 Day Total Sales']
    ad_product['Cost Per Click (CPC)'].fillna(value=0, inplace=True)
    ad_product['Spend'].fillna(value=0, inplace=True)
    ad_product['Click-Thru Rate (CTR)'].fillna(value=0, inplace=True)
    ad_product['Total Advertising Cost of Sales (ACoS)'].fillna(value=0, inplace=True)
    ad_product['7 Day Total Sales'].fillna(value=0, inplace=True)
    ad_product[['Spend', '7 Day Total Sales']] = ad_product[['Spend', '7 Day Total Sales']].applymap(
        lambda x: float(x) if x not in ['', ' '] else 0)
    ad_product['Cost Per Click (CPC)'] = ad_product['Cost Per Click (CPC)'].apply(
        lambda x: int(re.sub('[^0-9]', '', str(x)[0:4])) / 100 if x not in ['', ' '] else 0)
    init_cols_us = [col + '(US$)' for col in init_cols]
    ad_product[init_cols_us] = ad_product[init_cols].applymap(lambda x: round(exchange_rate[site] * x, 2))
    ad_product.rename(columns={col: col.replace(" ", "_") for col in ad_product.columns}, inplace=True)
    ad_product.rename(
        columns={'Advertised_SKU': 'SKU', 'Advertised_ASIN': 'ASIN', 'Click-Thru_Rate_(CTR)': 'ctr',
                 "Cost_Per_Click_(CPC)": 'cpc', '7_Day_Total_Orders_(#)': '7_Day_Total_Orders',
                 'Total_Advertising_Cost_of_Sales_(ACoS)': 'ACoS', 'Cost_Per_Click_(CPC)(US$)': 'cpc_us',
                 'Spend(US$)': 'Spend_us', '7_Day_Total_Sales(US$)': '7_Day_Total_Sales_us',
                 '7天总订单数(#)': '7 Day Total Orders (#)'}, inplace=True)
    ad_product = ad_product[
        ['account', 'country', 'station', 'year', 'month', 'Start_Date', 'End_Date', 'Portfolio_name', 'Currency',
         'Campaign_Name',
         'Ad_Group_Name', 'SKU', 'ASIN', 'Impressions', 'Clicks', 'ctr', 'cpc', 'Spend', '7_Day_Total_Sales',
         '7_Day_Total_Orders', 'ACoS', 'cpc_us', 'Spend_us', '7_Day_Total_Sales_us']]
    return ad_product


# 将ad_product数据上传到数据库中
def db_upload_ad_product(ad_product: pd.DataFrame, db='team_station', table_name='station_ad_product',
                         ip='wuhan.yibai-it.com',
                         user_name='marmot',
                         password='', port=33061):
    conn = pymysql.connect(
        host=ip,
        user=user_name,
        password=password,
        database=db,
        port=port,
        charset='UTF8')
    # 创建游标
    cursor = conn.cursor()
    now_datetime = str(datetime.now())
    ad_product['update_time'] = now_datetime
    station_name = ad_product['station'].values[0]
    year = ad_product['year'].values[0]
    month = ad_product['month'].values[0]
    ad_product['Start_Date'] = ad_product['Start_Date'].astype(str)
    ad_product['End_Date'] = ad_product['End_Date'].astype(str)
    # 将数据变成可进行读入数据库的dict格式
    all_list = []
    ad_product.reset_index(drop=True, inplace=True)
    # df = ad_product.astype(object).replace(pd.isna(), 'None')
    df = ad_product.values
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)
    # 写sql
    # 更新站点月份数据
    delete_sql = """delete from {} where (station = {}) and (year = {}) and (month = {}) """.format(table_name,
                                                                                                    "'%s'" % station_name,
                                                                                                    "%s" % year,
                                                                                                    "%s" % month)
    insert_sql = """insert into {} (account, site, station,year,month,Start_Date, End_Date,
       Portfolio_name, Currency, Campaign_Name, Ad_Group_Name, SKU,
       ASIN, Impressions, Clicks, ctr, cpc, Spend,
       7_Day_Total_Sales, 7_Day_Total_Orders, ACoS, cpc_us, Spend_us,
       7_Day_Total_Sales_us,update_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(
        table_name)

    cursor.execute(delete_sql)
    cursor.executemany(insert_sql, all_list)
    conn.commit()
    print(f'{station_name}更新成功')
    # except Exception as e:
    #     conn.rollback()
    #     print(f'{station_name}更新成功')
    #     print(e)


def old_clickMe():  # 当acction被点击时,该函数则生效
    action.configure(text='Hello ' + personChosen.get())  # 设置button显示的内容
    lab3.delete(0.0, tk.END)
    # action.configure(state='disabled')  # 将按钮设置为灰色状态，不可使用状态
    for i in ['mad', 'angry', 'sad']:
        showtxt = i
        lab3.insert('insert', showtxt + '\n')
        lab3.update()


def clickMe():
    action.configure(text='Hello ' + personChosen.get())  # 设置button显示的内容
    # action.configure(state='disabled')  # 将按钮设置为灰色状态，不可使用状态
    lab3.delete(0.0, tk.END)

    folder_path = address.get()
    month_flag = int(re.findall('.*年(.*)月.*', folder_path)[0])
    year_flag = int(re.findall('[0-9]{4}', folder_path)[0])
    # print("Month %s,Processing..." % month_flag)
    showtxt = "Month %s, Now Is Processing..." % month_flag
    lab3.insert('insert', showtxt + '\n')
    lab3.update()
    os.chdir(folder_path)

    # every station country get data------------------------------------------------------------------------------------
    df_br = pd.DataFrame()
    df_br_all = pd.DataFrame()
    df_cp = pd.DataFrame()
    df_cp_all = pd.DataFrame()
    df_ad_product = pd.DataFrame()
    df_ad_product_all = pd.DataFrame()

    error_file_dir = os.path.join(os.path.dirname(folder_path), 'error_msg.txt')

    if os.path.exists(error_file_dir):
        os.remove(error_file_dir)

    i = 0
    for name in os.listdir(folder_path):
        # account, country = (name.split("~")[0], name.split("~")[1].upper())
        account = re.findall('[A-Za-z0-9]+', name)[0].lower()
        country = re.findall('[A-Za-z0-9]+', name)[1].upper()
        station_name = account + '_' + country
        files_path = folder_path + r'\\' + name
        os.chdir(files_path)
        # 读取Br表
        if not glob.glob(files_path + '/*BusinessReport*.csv'):
            message = f'{station_name}缺失BusinessReport表.'
            i += 1
            with open(error_file_dir, mode="a+") as f:
                f.write(f"{i}:{message}" + "\n")
        for file in glob.glob('*BusinessReport*.csv'):
            df_br = pd.DataFrame(pd.read_csv(file, engine='python', encoding='utf_8_sig'))
            df_br.rename(columns={"日期": "Date",
                                  "已订购商品销售额": "Ordered Product Sales",
                                  "已订购商品的销售额 – B2B": "Ordered Product Sales – B2B",
                                  "已订购商品数量": "Units Ordered",
                                  "订购数量 – B2B": "Units Ordered – B2B",
                                  "订单商品种类数": "Total Order Items",
                                  "订单商品总数 – B2B": "Total Order Items – B2B",
                                  "页面浏览次数": "Page Views",
                                  "买家访问次数": "Sessions",
                                  "购买按钮赢得率": "Buy Box Percentage",
                                  "订单商品数量转化率": "Unit Session Percentage",
                                  "商品转化率 – B2B": "Unit Session Percentage – B2B",
                                  "平均在售商品数量": "Average Offer Count",
                                  "平均父商品数量": "Average Parent Items"}, inplace=True)
            df_br = df_br.reindex(columns=['Date', 'Ordered Product Sales', 'Units Ordered', 'Sessions'])
            df_br['account'] = account
            df_br['country'] = country
            df_br_all = df_br_all.append(df_br)
        # print(account + "_" + country + ",Business Report Done!!!")
        showtxt = account + "_" + country + ",Business Report Done!!!"
        lab3.insert('insert', showtxt + '\n')
        # lab3.update()
        lab3.see(tk.END)
        # 读取广告报表
        if not glob.glob(files_path + '/*CAMPAIGN*.csv'):
            message = f'{station_name}缺失CAMPAIGN表.'
            i += 1
            with open(error_file_dir, mode="a+") as f:
                f.write(f"{i}:{message}" + "\n")
        for file in glob.glob('*CAMPAIGN*'):
            if file[-3:] == 'csv':
                try:
                    df_cp = pd.DataFrame(pd.read_csv(file, engine='python', encoding='utf_8_sig'))
                except:
                    df_cp = pd.DataFrame(pd.read_csv(file, engine='python', encoding='ANSI'))
            else:
                df_cp = pd.DataFrame(pd.read_excel(file))
            df_cp['account'] = account
            df_cp['country'] = country
            # campaign 表头标准化
            df_cp.rename(columns=lambda x: re.sub('\\(.*?\\)|\\{.*?}|\\[.*?]', '', x), inplace=True)
            df_cp.rename(columns={'状态': 'State', '广告活动': 'Campaigns', '状态.1': 'Type', '类型': 'Status', '投放': 'Targeting',
                                  '广告活动的竞价策略': 'Campaign bidding strategy', '开始日期': 'Start date', '结束日期': 'End date',
                                  '广告组合': 'Portfolio',
                                  '预算': 'Budget', '曝光量': 'Impressions', '点击次数': 'Clicks', '点击率 ': 'CTR', '花费': 'Spend',
                                  '每次点击费用 ': 'CPC', '订单': 'Orders', '销售额': 'Sales', '广告投入产出比 ': 'ACos'}, inplace=True)
            df_cp = df_cp.reindex(
                columns=['State', 'Campaigns', 'Status', 'Type', 'Targeting', 'Start date', 'End date', 'Budget',
                         'Impressions', 'Clicks', 'CTR', 'Spend', 'CPC', 'Orders', 'Sales', 'ACoS', 'account',
                         'country'])
            df_cp_all = df_cp_all.append(df_cp)
        # print(account + "_" + country + ",Campaign Done!!!")
        showtxt = account + "_" + country + ",Campaign Done!!!"
        lab3.insert('insert', showtxt + '\n')
        lab3.see(tk.END)
        # 读取 Advertised product表
        '''
        if not glob.glob(files_path + '/*Advertised product*'):
            message = f'{station_name}缺失Advertised product表.'
            i += 1
            with open(error_file_dir, mode="a+") as f:
                f.write(f"{i}:{message}" + "\n")
        for file in glob.glob('*Advertised product*'):
            try:
                df_ad_product = pd.read_excel(file)
            except Exception as e:
                print(f'{file}表有问题，请查看.')
                print(e)
            df_ad_product['account'] = account
            df_ad_product['country'] = country
            station_name = account + '_' + country
            df_ad_product['station'] = station_name
            df_ad_product['year'] = year_flag
            df_ad_product['month'] = month_flag
            if df_ad_product.empty:
                continue
            # df_cp = df_cp.reindex(
            #     columns=['State', 'Campaigns', 'Status', 'Type', 'Targeting', 'Start date', 'End date', 'Budget',
            #              'Impressions', 'Clicks', 'CTR', 'Spend', 'CPC', 'Orders', 'Sales', 'ACoS', 'account',
            #              'country'])
            df_ad_product = init_ad_product(df_ad_product)
            db_upload_ad_product(df_ad_product)
            '''

    if os.path.exists(error_file_dir):
        with open(f"{error_file_dir}", "a+") as f:  # 打开文件
            f.write("==================================" + '\n')
            f.write("请下载完整表格后重新运行上传程序。")
        with open(f"{error_file_dir}", "r") as f:  # 打开文件
            data = f.read()
        tkinter.messagebox.showinfo(message=data)
        return

    # business report ,月份/年份/货币金额---------------------------------------------------------------------------------
    def str2month(df):
        if len(re.findall('[0-9]+', df['Date'])[0]) == 4:
            return re.findall('[0-9]+', df['Date'])[1]
        else:
            if df['country'] in ['US', 'CA', 'JP', 'MX']:
                return re.findall('[0-9]+', df['Date'])[0]
            else:
                return re.findall('[0-9]+', df['Date'])[1]

    def str2year(df):
        if len(re.findall('[0-9]+', df['Date'])[0]) == 4:
            return re.findall('[0-9]+', df['Date'])[0]
        else:
            return re.findall('[0-9]+', df['Date'])[2]

    # df_br_all['month'] = df_br_all.apply(lambda x: x['Date'][0:2] if (x['country'] in ['US', 'CA', 'JP', 'MX']) else x['Date'][3:5], axis=1)
    df_br_all = df_br_all.reset_index(drop=True)
    try:
        df_br_all['month'] = df_br_all.apply(lambda x: str2month(x), axis=1)
        df_br_all['year'] = df_br_all.apply(lambda x: str2year(x), axis=1)
        df_br_all['month'] = df_br_all['month'].astype('int')
        df_br_all['year'] = df_br_all['year'].astype('int')
        df_br_all['Ordered Product Sales'] = df_br_all['Ordered Product Sales'].str.extract('(\d+,?\d*.\d+)')
        for col in ['Ordered Product Sales', 'Units Ordered', 'Sessions']:
            df_br_all[col] = df_br_all[col].astype('str')
            df_br_all[col] = df_br_all[col].str.replace(',', '').astype('float')
        df_br_month = df_br_all[(df_br_all['month'] == month_flag) & (df_br_all['year'] == year_flag)]
    except Exception as e:
        info = traceback.format_exc()
        error_r = int(info[info.find('occurred at index ') + 18:][:-3])
        showtxt = df_br_all.iloc[error_r]['account'] + "_" + df_br_all.iloc[error_r]['country'] + ",BusinessReport有误"
        lab3.insert('insert', showtxt + '\n')
        lab3.see(tk.END)

    # campaign, 直接汇总生成--------------------------------------------------------------------------------------------
    def money2num(num):
        num = num.rstrip()
        if any(i in num for i in [',', '.']):  # 原数据中含有,.等符号
            res = ''
            for ii in filter(str.isdigit, num):
                res += ii
            if num[-3].isdigit():
                return float(res) / 10
            else:
                return float(res) / 100
        else:
            return float(num + '00') / 100

    def amount2num(num):
        res = ''
        for ii in filter(str.isdigit, num.split('.')[0]):
            res += ii
        return int(res)

    df_cp_all.dropna(subset=['Spend'], inplace=True)
    for col in ['Spend', 'Sales']:
        df_cp_all[col] = df_cp_all[col].astype('str')
        df_cp_all[col] = df_cp_all[col].apply(lambda x: money2num(x))

    for col in ['Clicks', 'Orders']:
        df_cp_all[col] = df_cp_all[col].astype('str')
        df_cp_all[col] = df_cp_all[col].apply(lambda x: amount2num(x))
    df1 = df_cp_all.groupby(['account', 'country'])['Clicks', 'Spend', 'Orders', 'Sales'].sum().reset_index()
    df1['ACoS'] = df1['Spend'] / df1['Sales']
    df1['CPC'] = df1['Spend'] / df1['Clicks']
    df1['CR'] = df1['Orders'] / df1['Clicks']

    # business merge campaign-------------------------------------------------------------------------------------------
    df_merge = df_br_month.merge(df1, on=['account', 'country'], how='outer')
    df_merge['account'] = df_merge['account'].str.upper()
    df_merge['month'] = df_merge['month'].astype('str') + '月'
    df_merge['year'] = str(year_flag) + '年'
    df_merge['account-country'] = df_merge['account'] + '-' + df_merge['country']
    df_merge['spend/sales'] = df_merge['Spend'] / df_merge['Ordered Product Sales']
    df_merge['Clicks/Sessions'] = df_merge['Clicks'] / df_merge['Sessions']
    df_merge['order percentage'] = df_merge['Orders'] / df_merge['Units Ordered']
    df_merge['session percentage'] = df_merge['Units Ordered'] / df_merge['Sessions']
    df_merge = df_merge.reindex(
        columns=['account', 'country', 'account-country', 'year', 'month', 'Spend', 'ACoS', 'Sales',
                 'Ordered Product Sales', 'spend/sales', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                 'Clicks', 'Sessions', 'Clicks/Sessions', 'Orders', 'Units Ordered',
                 'order percentage', 'CR', 'session percentage'])
    os.chdir(folder_path[:folder_path.rfind('\\')])
    df_merge.to_excel('alldata' + f'{year_flag}年' + str(month_flag) + '月-' + personChosen.get() + '.xlsx', index=False)
    showtxt = "All Files Are Done!!! 请关闭窗口并到地址的上一级查看汇总文件"
    lab3.insert('insert', showtxt + '\n')
    lab3.update()
    lab3.see(tk.END)
    # lab3.mark_set('insert',1.5)
    return None


# 按钮
action = ttk.Button(win, text="Ready? Go!", command=clickMe)  # 创建一个按钮, text：显示按钮上面显示的文字, command：当这个按钮被点击之后会调用command函数
action.grid(column=2, row=1)  # 设置其在界面中出现的位置  column代表列   row 代表行

# 输出框
showtxt = tk.StringVar()
lab3 = tk.Text(win, fg='blue')
# lab3 = tk.Label(win,textvariable = showtxt,height=10, width=50,fg='blue',bg='yellow')
lab3.grid(row=3, column=0, columnspan=3)

win.mainloop()  # 当调用mainloop()时,窗口才会显示出来
