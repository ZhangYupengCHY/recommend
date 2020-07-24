# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/12/26 17:21
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import rsa, json, requests, os, shutil, uuid
import numpy as np
import difflib
import win32api
import tkinter.messagebox
from tkinter import *
import pandas as pd
import random
import base64, pymysql
from datetime import datetime
import warnings
import io

warnings.filterwarnings(action='ignore')
country_name_translate = {'加拿大': '_ca', '墨西哥': '_mx', "美国": '_us', '日本': '_jp', '印度': '_in', '澳大利亚': '_au', '德国': "_de",
                          '法国': '_fr', '意大利': '_it', '西班牙': '_es', '英国': '_uk'}
file_load_drive = 'C'

sale_exchange_rate = {'CA': 0.7519, 'DE': 1.0981, 'FR': 1.0981, 'IT': 1.0981, 'SP': 1.0981, 'JP': 0.009302,
                      'UK': 1.2445, 'MX': 0.05147, 'IN': 0.01412, 'US': 1, 'ES': 1.0981, 'AU': 0.6766}

bid_exchange = {'CA': 1, 'DE': 1, 'FR': 1, 'IT': 1, 'SP': 1, 'JP': 0.009302,
                'UK': 1, 'MX': 0.05147, 'IN': 0.01412, 'US': 1, 'ES': 1, 'AU': 0.6766}

# 本币
ad_group_least_bid = {'CA': 0.02, 'DE': 0.02, 'FR': 0.02, 'IT': 0.02, 'SP': 0.02, 'JP': 2,
                      'UK': 0.02, 'MX': 0.1, 'IN': 0.1, 'US': 0.02, 'ES': 0.02, 'AU': 0.1}

acos_ideal = {'CA': 0.14, 'DE': 0.15, 'FR': 0.15, 'IT': 0.15, 'SP': 0.15, 'JP': 0.15,
              'UK': 0.18, 'MX': 0.15, 'IN': 0.18, 'US': 0.18, 'ES': 0.15, 'AU': 0.15}
# 本币
cpc_max = {'CA': 0.4, 'DE': 0.35, 'FR': 0.35, 'IT': 0.3, 'SP': 0.3, 'JP': 25,
           'UK': 0.4, 'MX': 2.5, 'IN': 4.5, 'US': 0.5, 'ES': 0.3, 'AU': 0.4}

# 站点的最小出价
ad_group_max_bid_lower_limit_dict = {'US': 0.02, 'CA': 0.02, 'MX': 0.1, 'UK': 0.02, 'DE': 0.02, 'FR': 0.02, 'IT': 0.02,
                                     'ES': 0.02, 'JP': 2, 'AU': 0.02, 'IN': 1, 'AE': 0.24}


# processed_files 1.处理精否/否定asin
def negative_exact(station_name, camp_data, st_data, active_listing_data, stations_folder):
    """
    描述:
        用户搜索的内容中,点击或是曝光产生不了收益，这些词或是ASIN我们可以认为是无效的,于是可以将
        这部分用户搜索词或是ASIN停掉,不再产生花费。
    逻辑:
        否定分为:处理精否(关键词keyword)和否定ASIN(ASIN)
        否定关键词主逻辑：
            # 条件1： click>40,order=0;
            # 条件2： spend>站点平均cpc*40,order=0;
            # 条件3： acos>站点平均acos*3,cr<2%;
            # 条件4：acos>站点平均acos*3,cr<3%,click>40*(order+1);
            # 条件5: acos>站点平均acos*3,spend>站点平均cpc*40*(order+1)
        否定ASIN主逻辑:
            # 通过click去否定
    :param station_name:站点名
    :param camp_data:广告报表
    :param st_data:搜索词报表
    :param active_listing_data:用于sku和asin之间的关系
    :param stations_folder:站点文件夹存储的路径
    :return:
    """

    # 连接数据库得到站点平均cpc,acos,返回station,acos,cpc三列
    def db_download_station_names(db='team_station', table='only_station_info', ip='wuhan.yibai-it.com', port=33061,
                                  user_name='marmot', password=''):
        """
        加载广告组接手的站点以及对应的平均数据
        :return: 站点平均cpc和acos
        """
        conn = pymysql.connect(
            host=ip,
            user=user_name,
            password=password,
            database=db,
            port=port,
            charset='UTF8')
        # 创建游标
        cursor = conn.cursor()
        # 写sql
        sql = """SELECT station,acos,cpc FROM {} """.format(table)
        # 执行sql语句
        cursor.execute(sql)
        stations_avg = cursor.fetchall()
        stations_avg = pd.DataFrame([list(station) for station in stations_avg], columns=['station', 'acos', 'cpc'])
        stations_avg.drop_duplicates(inplace=True)
        conn.commit()
        cursor.close()
        conn.close()
        return stations_avg

    # 按照条件处理需要精否的keywords,生成精否过程表
    def get_negative_exact_kws(station_name, st_data, camp_data, stations_folder):
        if st_data is None:
            return
        if st_data.empty:
            return
        # 保留关键词,剔除掉B0,b0开头(ASIN否定)
        site = station_name[-2:].upper()
        stations_info = db_download_station_names()
        station_acos = stations_info['acos'][stations_info['station'] == station_name.lower()]
        if station_acos.empty:
            station_acos = None
            print(f'站点不存在:站点{station_name}无法在only_station_info中找到.')
            return
        else:
            station_acos = station_acos.values[0]
            station_acos = float(station_acos.replace('%', '')) / 100
            # 防止站点acos过高，设置一个上限
            station_acos = min(station_acos, acos_ideal[site] * 2)

        station_cpc = stations_info['cpc'][stations_info['station'] == station_name.lower()]
        if station_cpc.empty:
            station_cpc = None
            print(f'站点不存在:站点{station_name}无法在only_station_info中找到.')
            return
        else:
            station_cpc = station_cpc.values[0]
            # 防止站点cpc过高，设置一个上限
            station_cpc = min(station_cpc, cpc_max[site])

        # 筛选出需要的列
        # clicks,spend,7 day total orders,advertising cost of sales(acos)
        need_columns = ['Campaign Name', 'Ad Group Name', 'Customer Search Term', 'Match Type',
                        'Impressions', 'Clicks', 'Spend', '7 Day Total Orders (#)', '7 Day Total Sales',
                        'Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate']
        if not set(need_columns).issubset(set(st_data.columns)):
            print(f'缺失列: 站点 {station_name} st表缺失{set(need_columns) - set(st_data.columns)}列。')
            return
        # 将acos和cr转化成数值型
        st_data[['Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate']] = st_data[
            ['Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate']].applymap(
            lambda x: x if not isinstance(x, str) else float(x) if '%' not in x else float(x.replace('%', '')) / 100)

        # st表中的关键词搜索
        st_keywords_data = st_data[~st_data['Customer Search Term'].str.startswith(('B0', 'b0'))]
        # 查找满足条件的关键词

        # 条件1： click>40,order=0
        negative_exact_kws_1 = st_keywords_data[need_columns][
            (st_keywords_data['Clicks'] > 40) & (st_keywords_data['7 Day Total Orders (#)'] == 0)]
        # 条件2： spend>站点平均cpc*40,order=0
        if station_cpc is not None:
            negative_exact_kws_2 = st_keywords_data[need_columns][
                (st_keywords_data['Spend'] > station_cpc * 40) & (st_keywords_data['7 Day Total Orders (#)'] == 0)]
        else:
            negative_exact_kws_2 = None
        # 条件3： acos>站点平均acos*3,cr<2%;
        if station_acos is not None:
            negative_exact_kws_3 = st_keywords_data[need_columns][
                (st_keywords_data['Advertising Cost of Sales (ACoS)'] > station_acos * 3) & (
                        st_keywords_data['7 Day Conversion Rate'] < 0.02)]
        else:
            negative_exact_kws_3 = None
        # 条件4：acos>站点平均acos*3,cr<3%,click>40*(order+1);
        if station_acos is not None:
            negative_exact_kws_4 = st_keywords_data[need_columns][
                (st_keywords_data['Advertising Cost of Sales (ACoS)'] > station_acos * 3) & (
                        st_keywords_data['7 Day Conversion Rate'] < 0.03) &
                (st_keywords_data['Clicks'] > (st_keywords_data['7 Day Total Orders (#)'] + 1) * 40)]
        else:
            negative_exact_kws_4 = None
        # 条件5: acos>站点平均acos*3,spend>站点平均cpc*40*(order+1)
        if station_acos is not None:
            negative_exact_kws_5 = st_keywords_data[need_columns][
                (st_keywords_data['Advertising Cost of Sales (ACoS)'] > station_acos * 3) &
                (st_keywords_data['Spend'] > (st_keywords_data['7 Day Total Orders (#)'] + 1) * 40 * station_cpc)]
        else:
            negative_exact_kws_5 = None
        all_negative_exact_kws = [kws for kws in [negative_exact_kws_1, negative_exact_kws_2, negative_exact_kws_3,
                                                  negative_exact_kws_4, negative_exact_kws_5] if kws is not None]
        if all_negative_exact_kws:
            all_negative_exact_kws = pd.concat(all_negative_exact_kws)

        # st表中的ASIN搜索
        st_asin_data = st_data[st_data['Customer Search Term'].str.startswith(('B0', 'b0'))]
        # 查找满足条件的asin
        negative_exact_asin = st_asin_data[need_columns][st_asin_data['Clicks'] > 1]

        # 将asin否定关键词和keyword否定关键词合并处理
        all_negative_exact = pd.concat([all_negative_exact_kws, negative_exact_asin])

        all_negative_exact.drop_duplicates(inplace=True)
        # 输出精否临时表
        now_date = datetime.now().date()
        # 删除camp表中重复的精否词
        if (camp_data is not None) & (not camp_data.empty):
            # 处理camp表的列名
            camp_data.columns = [column.strip(" ") for column in camp_data.columns]
            camp_negative_exact_data = camp_data[
                (camp_data['Match Type'].str.contains('negative', case=False)) & (
                        camp_data['Campaign Status'] == 'enabled') &
                (camp_data['Ad Group Status'] == 'enabled') & (camp_data['Status'] == 'enabled')]
            if not camp_negative_exact_data.empty:
                for index, row in all_negative_exact.iterrows():
                    st_camp_name = row['Campaign Name']
                    st_group_name = row['Ad Group Name']
                    st_kws = row['Customer Search Term']
                    camp_negative_exact_haved = camp_negative_exact_data[
                        (camp_negative_exact_data['Campaign'] == st_camp_name)
                        & (camp_negative_exact_data['Ad Group'] == st_group_name)
                        & (camp_negative_exact_data['Keyword or Product Targeting'] == st_kws)]
                    if not camp_negative_exact_haved.empty:
                        all_negative_exact.drop(index=index, inplace=True)
        all_negative_exact.reset_index(drop=True, inplace=True)
        # 通过camp表 添加Max bid和Status两列
        for index in all_negative_exact.index:
            max_bid = camp_data['Max Bid'][
                (camp_data['Campaign'] == all_negative_exact.loc[index, 'Campaign Name']) &
                (camp_data['Ad Group'] == all_negative_exact.loc[index, 'Ad Group Name']) &
                (camp_data['Keyword or Product Targeting'] == all_negative_exact.loc[
                    index, 'Customer Search Term'])]
            if not max_bid.empty:
                all_negative_exact.loc[index, 'Max bid'] = max_bid.values[0]
            else:
                all_negative_exact.loc[index, 'Max bid'] = None

            status = camp_data['Status'][(camp_data['Campaign'] == all_negative_exact.loc[index, 'Campaign Name']) &
                                         (camp_data['Ad Group'] == all_negative_exact.loc[index, 'Ad Group Name']) &
                                         (camp_data['Keyword or Product Targeting'] == all_negative_exact.loc[
                                             index, 'Customer Search Term'])]
            if not status.empty:
                all_negative_exact.loc[index, 'Status'] = status.values[0]
            else:
                camp_da_group_status = camp_data['Ad Group Status'][
                    (camp_data['Campaign'] == all_negative_exact.loc[index, 'Campaign Name']) &
                    (camp_data['Ad Group'] == all_negative_exact.loc[index, 'Ad Group Name'])]
                if not camp_da_group_status.empty:
                    all_negative_exact.loc[index, 'Status'] = camp_da_group_status.values[0]

                else:
                    all_negative_exact.loc[index, 'Status'] = None

            export_columns = ['Campaign Name', 'Ad Group Name', 'Customer Search Term', 'Max bid', 'Match Type',
                              'Impressions', 'Clicks', 'Spend', '7 Day Total Orders (#)', '7 Day Total Sales',
                              'Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate', 'Status']
            all_negative_exact = all_negative_exact[export_columns]
            all_negative_exact.drop_duplicates(inplace=True)

            # 输出成excel
        now_date = datetime.now().date()
        processed_folder_name = 'processed_files'
        file_save_folder = os.path.join(stations_folder, station_name, processed_folder_name)
        if not os.path.exists(file_save_folder):
            os.makedirs(file_save_folder)
        now_date = datetime.now().strftime('%y.%m.%d')
        file_basename = f'{now_date} {station_name.upper()} 精否过程表.xlsx'
        file_save_path = os.path.join(file_save_folder, file_basename)

        # 输出精否逻辑
        def negative_exact_logic(avg_cpc, avg_acos):
            '''
            logic1:click>40,order=0
            logic2:spend>站点平均cpc*40,order=0
            logic3:acos>站点平均acos*3,cr<2%
            logic4:acos>站点平均acos*3,cr<3%,click>40*(order+1)
            logic5:acos>站点平均acos*3,spend>站点平均cpc*(order+1)
            '''
            negative_exact_logic = pd.DataFrame([['click>40 & order=0', f'spend>{round(avg_cpc * 40, 2)} & order=0',
                                                  f'acos>{round(avg_acos * 3, 2)} & cr<2%',
                                                  f'acos>{round(avg_acos * 3, 2)} & cr<3% & click>40*(order+1)',
                                                  f'acos>{round(avg_acos * 3, 2)} & sp\
                                                                        end>{round(avg_cpc, 2)}*(order+1)']],
                                                columns=['logic1', 'logic2', 'logic3', 'logic4', 'logic5'])
            return negative_exact_logic
            # negative_exact_logic.to_excel(file_save_path, index=False, sheet_name='精否条件')

        # 输出精否过程表和精否条件
        def export_negative_exact_logic_n_kws(all_negative_exact_kws, negative_exact_logic, file_save_path):
            if (all_negative_exact_kws.empty) or (all_negative_exact_kws is None):
                return
            write = pd.ExcelWriter(file_save_path)
            all_negative_exact_kws.to_excel(write, sheet_name='精否过程表', index=False)
            negative_exact_logic.to_excel(write, sheet_name='精否条件', index=False)
            write.save()

        # 输出精否过程表中精否条件
        negative_exact_logic_info = negative_exact_logic(station_cpc, station_acos)
        export_negative_exact_logic_n_kws(all_negative_exact, negative_exact_logic_info, file_save_path)
        return all_negative_exact

    # 生成精否表
    def negative_kws_file(station_name, negative_exact_kws_data, camp_data, active_listing_data, stations_folder):
        if negative_exact_kws_data is None:
            return
        if negative_exact_kws_data.empty:
            return
        std_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                       'Campaign Targeting Type', 'Ad Group Name',
                       'Max Bid', 'SKU', 'Keyword', 'Product Targeting ID', 'Match Type', 'Campaign Status',
                       'Ad Group Status', 'Status', 'Bidding strategy']
        negative_exact_kws_upload_format = pd.DataFrame(columns=std_columns)
        for index, row in negative_exact_kws_data.iterrows():
            negative_exact_kw_upload_format_temp = pd.DataFrame([[None] * len(std_columns)], columns=std_columns)
            negative_exact_kw_upload_format_temp['Campaign Name'] = row['Campaign Name']
            negative_exact_kw_upload_format_temp['Ad Group Name'] = row['Ad Group Name']
            negative_exact_kw_upload_format_temp['Keyword'] = row['Customer Search Term']
            negative_exact_kws_upload_format = negative_exact_kws_upload_format.append(
                negative_exact_kw_upload_format_temp)
        negative_exact_kws_upload_format['Match Type'] = 'negative exact'
        negative_exact_kws_upload_format[['Campaign Status', 'Ad Group Status', 'Status']] = 'enabled'

        # 由于ad group name列存在命名不规范的情况(ad group name命名规范为sku asin),于是重新调整ad group的命名

        # 分开处理否定关键词和否定ASIN,由于否定ASIN需要打在定向ASIN下面打,于是改变否定ASIN的输出格式
        camp_data_sku_info = camp_data[['Campaign', 'Ad Group', 'SKU']][pd.notnull(camp_data['SKU'])]
        active_listing_data_sku_asin = active_listing_data[['seller-sku', 'asin1']]
        # 得到正确的SKU,添加SKU列
        negative_exact_kws_upload_format = pd.merge(negative_exact_kws_upload_format, camp_data_sku_info,
                                                    left_on=['Campaign Name', 'Ad Group Name'],
                                                    right_on=['Campaign', 'Ad Group'], how='left')
        # 得到正确的ASIN,添加ASIN列
        negative_exact_kws_upload_format = pd.merge(negative_exact_kws_upload_format, active_listing_data_sku_asin,
                                                    left_on=['SKU_y'], right_on=['seller-sku'], how='left')
        negative_exact_kws_upload_format['Ad Group Name'] = negative_exact_kws_upload_format['SKU_y'] + ' ' + \
                                                            negative_exact_kws_upload_format['asin1']
        negative_exact_kws_upload_format.rename(columns={'SKU_x': 'SKU'}, inplace=True)
        negative_exact_kws_upload_format = negative_exact_kws_upload_format[
            pd.notnull(negative_exact_kws_upload_format['Ad Group Name'])]
        negative_exact_kws_upload_format = negative_exact_kws_upload_format[std_columns]

        if not negative_exact_kws_upload_format.empty:

            # 精否词数据
            negative_exact_kw = negative_exact_kws_upload_format[
                ~negative_exact_kws_upload_format['Keyword'].str.contains('b0', case=False)]
            # 否定ASIN
            negative_asin_all_rows = negative_exact_kws_upload_format[
                negative_exact_kws_upload_format['Keyword'].str.contains('b0', case=False)]

        else:
            return

        if not negative_asin_all_rows.empty:

            # 否定asin中添加ad行,sku行和定向asin行
            def bulid_negative_asin_format(negative_asin_row: 'pd.Series', station_site,
                                           unknown_station_ad_group_max_bid_lower_limit=0.02):
                """
                否定asin为一行,添加对应的ad gruop行,sku行以及创造一个自身对应的定向asin
                ,同时出价给站点的最小出价
                :param negative_asin_row: 一个否定asin行
                :return: 充填完全的一个asin
                """
                station_site_list = ad_group_max_bid_lower_limit_dict.keys()

                if station_site.upper() in station_site_list:
                    station_ad_group_max_bid_lower_limit = ad_group_max_bid_lower_limit_dict[station_site]

                else:
                    print(f"UNKNOWN SITE: {station_site} 未知.")
                    print("广告组出价暂时给0.02,请及时添加新站点信息")
                    station_ad_group_max_bid_lower_limit = unknown_station_ad_group_max_bid_lower_limit

                # campaign name为固定值
                campaign_name = 'Negative Targeting Expression-SP_Bulk'
                ad_group_name = negative_asin_row['Ad Group Name']
                negative_asin_row_index = negative_asin_row.index
                empty_row = [None] * len(negative_asin_row)
                # find asin
                asin = re.findall('[Bb]0.{8}', ad_group_name)
                # 若没有asin,则返回
                if not asin:
                    return pd.Series(empty_row, index=negative_asin_row_index)
                ad_group_asin_expression = f'ASIN="{asin[-1].upper()}"'
                sku = ad_group_name.split(' ')[0]
                # ad_group_name是sku加asin
                ad_group_name = f'{sku} {asin[-1]}'
                empty_row = [None] * len(negative_asin_row)
                # first row(ad 行)
                ad_group_row = pd.Series(empty_row, index=negative_asin_row_index)
                ad_group_row['Max Bid'] = station_ad_group_max_bid_lower_limit
                ad_group_row['Campaign Status'] = 'enabled'
                ad_group_row['Ad Group Status'] = 'enabled'
                # second row(sku 行)
                sku_row = pd.Series(empty_row, index=negative_asin_row_index)
                sku_row['SKU'] = sku
                sku_row['Campaign Status'], sku_row['Ad Group Status'], sku_row[
                    'Status'] = 'enabled', 'enabled', 'enabled'
                # third row(创造的定向asin行)
                create_asin_row = pd.Series(empty_row, index=negative_asin_row_index)
                create_asin_row['Max Bid'] = station_ad_group_max_bid_lower_limit
                create_asin_row['Keyword'], create_asin_row[
                    'Product Targeting ID'] = ad_group_asin_expression, ad_group_asin_expression
                create_asin_row['Match Type'] = 'Targeting Expression'
                create_asin_row['Campaign Status'], create_asin_row['Ad Group Status'], create_asin_row[
                    'Status'] = 'enabled', 'enabled', 'enabled'
                # fourth row(第四行为否定asin行)
                negative_asin = negative_asin_row['Keyword']
                negative_asin_expression = f'ASIN="{negative_asin.upper()}"'
                negative_asin_row['Keyword'], negative_asin_row[
                    'Product Targeting ID'] = negative_asin_expression, negative_asin_expression
                negative_asin_row['Match Type'] = 'Negative Targeting Expression'

                # 合并四行
                one_negative_asin_df = pd.concat([ad_group_row, sku_row, create_asin_row, negative_asin_row], axis=1).T
                # 添加Campaign 行和ad group行
                one_negative_asin_df['Campaign Name'] = campaign_name
                one_negative_asin_df['Ad Group Name'] = ad_group_name
                return one_negative_asin_df

                # # 否定ASIN的Campaign Name是固定写法"Negative Targeting Expression-SP_Bulk"

            station_site = station_name[-2:].upper()
            # 循环否定asin中的每一行
            all_format_negative_asin = list(
                map(bulid_negative_asin_format, [negative_asin_row for _, negative_asin_row in
                                                 negative_asin_all_rows.iterrows()],
                    [station_site] * len(negative_asin_all_rows)))
            all_format_negative_asin = pd.concat(all_format_negative_asin)
            all_negative_data = pd.concat([negative_exact_kw, all_format_negative_asin])

        else:
            all_negative_data = negative_exact_kw

        # 输出成excel
        now_date = datetime.now().date()
        processed_folder_name = 'processed_files'
        file_save_folder = os.path.join(stations_folder, station_name, processed_folder_name)
        if not os.path.exists(file_save_folder):
            os.makedirs(file_save_folder)
        now_date = datetime.now().strftime('%y.%m.%d')
        file_basename = f'{now_date} {station_name.upper()} 精确否定词.xlsx'
        file_save_path = os.path.join(file_save_folder, file_basename)
        # print(file_save_path)
        all_negative_data.drop_duplicates(inplace=True)
        all_negative_data.reset_index(drop=True, inplace=True)
        all_negative_data.to_excel(file_save_path, index=False, sheet_name='精否')

    # 生成精否过程表
    negative_exact_kws_temp = get_negative_exact_kws(station_name, st_data, camp_data, stations_folder)

    # 生成精否表
    negative_kws_file(station_name, negative_exact_kws_temp, camp_data, active_listing_data, stations_folder)


# processed_files 2.处理新品自动新增
def process_auto_new(process_station, files_save_dirname, active_listing_data, all_order_data, camp_data):
    # 充填空的sku,并获取sku
    def get_cmap_sku(camp_data):
        camp_data.columns = [column.strip(' ') for column in camp_data.columns]
        if not set(['Record Type', 'Ad Group', 'SKU']).issubset(set(camp_data.columns)):
            lose_column = set(['Record Type', 'Ad Group', 'SKU']) - set(camp_data.columns)
            print(f'{process_station}:camp表缺失{lose_column}')
            return
        ad_info = camp_data[camp_data['Record Type'] == 'Ad']
        ad_group_list = ad_info['Ad Group']
        sku_list = ad_info['SKU']
        camp_sku_set = set([sku if pd.notna(sku) else ad_group.split(' ')[0] for ad_group, sku in
                            zip(ad_group_list, sku_list)])
        # 删除某些元素
        if 'ad' in camp_sku_set:
            camp_sku_set.remove('ad')
        return camp_sku_set

    def get_active_listing_info(active_listing_data):
        active_listing_data.columns = [column.strip(' ').lower() for column in active_listing_data.columns]
        if not set(['seller-sku', 'asin1', 'price', 'fulfillment-channel', 'open-date']).issubset(
                active_listing_data.columns):
            print(f'{process_station}:active_listing缺失seller_sku/asin/price/fulfillment_channel')
        # active_listing_data['seller_sku'] = active_listing_data['seller_sku'].apply(lambda x: x.lower())
        active_listing_sku_set_asin = active_listing_data[
            ['seller-sku', 'asin1', 'price', 'fulfillment-channel', 'open-date']]

        return active_listing_sku_set_asin

    def get_all_order_sku(all_order_data, site):
        sales_channel = {'it': 'Amazon.it', 'de': 'Amazon.de', 'es': 'Amazon.es', 'fr': 'Amazon.fr',
                         'uk': 'Amazon.co.uk', 'jp': 'Amazon.co.jp', 'us': 'Amazon.com', 'ca': 'Amazon.ca',
                         'mx': 'Amazon.com.mx', 'in': 'Amazon.in', 'au': 'Amazon.com.au'}
        all_order_data.columns = [column.strip(' ') for column in all_order_data.columns]
        if not set(['sales-channel', 'order-status', 'sku']).issubset(set(all_order_data.columns)):
            lose_column = set(['sales-channel', 'order-status', 'sku']) - set(all_order_data.columns)
            print(f'{process_station}:all_order表缺失{lose_column}')
            return
        site_sales_channel = sales_channel[site]
        all_order_sku = all_order_data[(all_order_data['sales-channel'] == site_sales_channel) & (
                all_order_data['order-status'] != 'Cancelled')]['sku']
        all_order_sku_set = set(all_order_sku)
        return all_order_sku_set

    def new_listing_upload_format(station_name, new_sku_list, active_listing_info, new_ao_listing, max_bid=0.4):
        export_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                          'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword',
                          'Product Targeting ID',
                          'Match Type', 'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy', 'Price',
                          'Fulfillment_channel', 'Start Date']
        # 竞价为空，bid = price * 15% * 3%
        # auto_new_data.reset_index(drop=True, inplace=True)
        station_name = station_name.upper()
        # start_date = datetime.now().strftime('%Y%m%d')
        file_date = datetime.now().strftime('%Y%m%d')[2:]
        camp_name_listing = f"AUTO-{station_name}-by-SP_Bulk-New"
        camp_name_ao = f"AUTO-{station_name}-by-SP_Bulk"
        country = station_name[-2:]
        listing_sku_upload_data = pd.DataFrame(columns=export_columns)
        ao_sku_upload_data = pd.DataFrame(columns=export_columns)

        def trans_sku_into_upload(sku, camp_name):
            asin = active_listing_info['asin1'][active_listing_info['seller-sku'] == sku].values[0]
            price = active_listing_info['price'][active_listing_info['seller-sku'] == sku].values[0]
            fulfillment = active_listing_info['fulfillment-channel'][active_listing_info['seller-sku'] == sku].values[0]
            start_date = active_listing_info['open-date'][active_listing_info['seller-sku'] == sku].values[0]
            if 'def' in fulfillment.lower():
                fulfillment = 'fbm'
            else:
                fulfillment = 'fba'
            bid = round(min(float(price) * 0.15 * 0.03, max_bid / bid_exchange[country]), 2)
            empty_list = [np.nan] * len(export_columns)
            processed_auto_new_data = pd.DataFrame([empty_list, empty_list], columns=export_columns)
            processed_auto_new_data['Campaign Name'] = camp_name
            processed_auto_new_data['Ad Group Name'] = "%s %s_%s" % (sku, asin, file_date)
            processed_auto_new_data.loc[0, 'Max Bid'] = bid
            processed_auto_new_data.loc[1, 'SKU'] = sku
            processed_auto_new_data.loc[0, 'Ad Group Status'] = 'enabled'
            processed_auto_new_data.loc[1, 'Status'] = 'enabled'
            # 添加客单价,发货方式,上架时间
            processed_auto_new_data['Price'] = price
            processed_auto_new_data['Fulfillment-channel'] = fulfillment
            processed_auto_new_data['Start Date'] = start_date
            return processed_auto_new_data

        if new_sku_list:
            listing_sku_upload_data = pd.concat([trans_sku_into_upload(sku, camp_name_listing) for sku in new_sku_list])
            listing_sku_upload_data['new_add_type'] = 'listing'
        if new_ao_listing:
            ao_sku_upload_data = pd.concat([trans_sku_into_upload(sku, camp_name_ao) for sku in new_ao_listing])
            ao_sku_upload_data['new_add_type'] = 'ao'
        # 添加新增类型并将两种类型合并

        if (not listing_sku_upload_data.empty) & (not ao_sku_upload_data.empty):
            all_sku_upload_data = pd.concat([ao_sku_upload_data, listing_sku_upload_data])
        elif not listing_sku_upload_data.empty:
            all_sku_upload_data = listing_sku_upload_data
        elif not ao_sku_upload_data.empty:
            all_sku_upload_data = ao_sku_upload_data
        else:
            return
        # 筛选出客单价10美金的SKU
        all_sku_upload_data = all_sku_upload_data[all_sku_upload_data['Price'] >= 10 / (sale_exchange_rate[country])]
        return all_sku_upload_data

    try:
        # 广告报表
        camp_sku_set = get_cmap_sku(camp_data)

        # 生成上传表
        active_listing_info = get_active_listing_info(active_listing_data)
        active_listing_sku_set = set(active_listing_info['seller-sku'])
        site = process_station[-2:].lower()
        all_order_sku_set = get_all_order_sku(all_order_data, site)
        new_sku = active_listing_sku_set - camp_sku_set
        new_sku_num = len(new_sku)
        # listing新增sku
        new_listing_sku = new_sku - all_order_sku_set
        # ao新增sku
        ao_listing_sku = new_sku & all_order_sku_set
        new_listing_upload_data = new_listing_upload_format(process_station, new_listing_sku, active_listing_info,
                                                            ao_listing_sku)
        fba_num = len(new_listing_upload_data[(pd.notna(new_listing_upload_data['SKU'])) & (
                new_listing_upload_data['Fulfillment-channel'] == 'fba')])
        fbm_num = len(new_listing_upload_data[(pd.notna(new_listing_upload_data['SKU'])) & (
                new_listing_upload_data['Fulfillment-channel'] == 'fbm')])
        now_datetime = datetime.now().strftime('%Y%m%d')

        # 2.将FBA新增大于30的存储到一个文件夹加下
        manager_folder = os.path.join(files_save_dirname, 'new_listing_auto_create(FBA_ABOVE_30)')
        if fba_num >= 30:
            # 得到站点对应的站点名
            if not os.path.exists(manager_folder):
                os.makedirs(manager_folder)
            new_listing_upload_data_path = os.path.join(manager_folder,
                                                        f"{process_station}_{now_datetime}_fba{fba_num} fbm{fbm_num}.csv")
            new_listing_upload_data.to_csv(new_listing_upload_data_path, index=False)
            print(f'{process_station}: 自动广告新增上传表完成.')
    except Exception as e:
        print(e)
        print(f'{process_station}: 自动广告新增上传表失败.')


# processed_files 3.关键词新增和ASIN新增
def process_st_new(station_name, st_data, camp_data, active_listing_data):
    """
    逻辑:
        1.出单和出单词的初步处理：
            1. 排除B0  2. 7天成交订单 > 0
        2. 去重: SKU + 关键词 + 匹配方式 已经做过的关键词去重、
        3. 在售: 即active_listing中要有这个SKU
        4.出价逻辑
        以下出价为广泛匹配出价，而精准出价为广泛出价+0.01，其中要是对应的Match Type出价才可以
        自动组出价：
        1. 出单且acos小于指定acos
            1.1 acos小于指定acos*0.1
                1.1.1 出单数为1: 关键词cpc+0.02
                1.1.2 出单数(1,5): 所在自动组出价+0.03
                1.1.3 出单数大于等于5: 所在自动组出价+0.04
            1.2 acos小于指定acos*0.3
                1.2.1 出单数为1: 关键词cpc+0.01
                1.2.2 出单数(1,5): 所在自动组出价+0.01
                1.2.3 出单数大于等于5: 所在自动组出价+0.02
            1.3 acos小于指定acos
                1.3.1 出单数为1: 关键词cpc
                1.3.2 出单数(1,5): 所在自动组出价
                1.3.3 出单数大于等于5: 所在自动组出价+0.01
        2.出单且acos大于指定acos
                关键词cpc*(指定acos/关键词acos)
        手动组出价:
        1. 出单且acos小于指定acos
            1.1 acos小于指定acos*0.1
                1.1.1 出单数为1: 关键词出价+0.02
                1.1.2 出单数(1,5): 关键词出价+0.03
                1.1.3 出单数大于等于5: 关键词出价+0.04
            1.2 acos小于指定acos*0.3
                1.2.1 出单数为1: 关键词出价+0.01
                1.2.2 出单数(1,5): 关键词出价+0.01
                1.2.3 出单数大于等于5: 关键词出价+0.02
            1.3 acos小于指定acos
                1.3.1 出单数为1: 关键词出价
                1.3.2 出单数(1,5): 关键词出价
                1.3.3 出单数大于等于5: 关键词出价+0.01
        2.出单且acos大于指定acos
            关键词cpc*(指定acos/关键词acos)


    步骤:
        1.初始化:判断st_data,以及camp_data是否正常
    :param st_data:ST原始数据
    :param camp_data:广告报表原始数据
    :return:ST新增的上传表
    """

    def detect_process_data_error(st_data, camp_data):
        """
        检测st_data和camp_data:若为空或是不是DataFrame或是为空
        :param datas: 检测st_data和camp_data等
        :return: false or true
        """
        datas = [st_data, camp_data]
        for data in datas:
            if (data is None) or (not isinstance(data, pd.DataFrame)) or (data.empty):
                return False
        return True

    def init_st_data(st_ori_data):
        """
        描述:
            从原始的st数据中筛选出需要的st数据
        逻辑:
            1. 排除Customer Search Term列中开头为B0且为10位数(BO开头不是关键词)  2. 7天成交订单 > 0
        :param st_ori_data:原始的st data
        :return: 筛选后的st data
        """
        # 1.判断数据是否正确
        if (st_ori_data is None) or (not isinstance(st_ori_data, pd.DataFrame)) or (st_ori_data.empty):
            return
        # 2.判断数据的列是否存在
        st_ori_data.columns = [col.strip(' ') for col in st_ori_data.columns]
        columns = st_ori_data.columns
        need_columns = {'Customer Search Term', '7 Day Total Sales'}
        st_ori_data.columns = ['7 Day Total Sales' if '7 Day Total Sales' in col else col for col in
                               st_ori_data.columns]
        if not need_columns.issubset(columns):
            print(f'lost columns:{station_name} 的st表缺少 {need_columns - set(columns)}')
            return
        # 筛除掉开头为B0且为10位数的搜索词
        # st_data_temp = st_ori_data[~st_ori_data['Customer Search Term'].str.contains('B0',case=False)]
        # 删除掉7天成交量订单大于0
        st_data_temp = st_ori_data[st_ori_data['7 Day Total Sales'] > 0]
        st_data_temp['Match Type'] = st_data_temp['Match Type'].apply(lambda x: x.lower())

        return st_data_temp

    def get_one_to_one_camp_data(camp_ori_data):
        """
        描述：
            1.从原始的camp中筛选出一个ad_group只有一个sku的数据，
            2. 将max bid向下充填
        逻辑:
            1.将campaign/ad_group汇总 如果sku个数大于1就剔除掉
        :param camp_ori_data: 原始的广告报表
        :return: 处理好之后的广告报表
        """
        # 1.判断st是否有效
        if (camp_ori_data is None) or (not isinstance(camp_ori_data, pd.DataFrame)) or (camp_ori_data.empty):
            print(f'camp data error: {station_name}')
            return
        # 2.判断数据的列是否存在
        columns = camp_ori_data.columns
        need_columns = {'Record Type', 'Campaign', 'Ad Group', 'SKU', 'Max Bid'}
        if not need_columns.issubset(columns):
            print(f'lost columns:{station_name} 的camp表缺少 {need_columns - columns}')
            return
        # 3. 筛选出ad行
        camp_ori_data['Campaign Targeting Type'].fillna(method='ffill', inplace=True)
        camp_ori_data['Match Type'] = camp_ori_data['Match Type'].str.lower()
        camp_ori_data_ad_row = camp_ori_data[camp_ori_data['Record Type'] == 'Ad']
        # 4.先找到一对一的camp,ad_group
        ad_group_type = camp_ori_data_ad_row.groupby(['Campaign', 'Ad Group']).agg({'SKU': 'count'}).reset_index()
        ad_group_one_to_one = ad_group_type[ad_group_type['SKU'] == 1]
        # 5.筛选出camp中一对一的数据
        ad_group_one_to_one['camp_ad_group'] = ad_group_one_to_one['Campaign'] + ad_group_one_to_one['Ad Group']
        ad_group_one_to_one_set = set(ad_group_one_to_one['camp_ad_group'])
        camp_ori_data['camp_ad_group'] = camp_ori_data['Campaign'] + camp_ori_data['Ad Group']
        camp_data_one_to_one_temp = camp_ori_data[camp_ori_data['camp_ad_group'].isin(ad_group_one_to_one_set)]
        camp_data_one_to_one = camp_data_one_to_one_temp.copy()
        del camp_data_one_to_one['camp_ad_group']

        # 向下充填max bid
        camp_data_one_to_one['Max Bid'].fillna(method='ffill', inplace=True)
        # 填充sku
        one_group_data = []
        ad_group_grouped = camp_data_one_to_one.groupby(['Campaign', 'Ad Group'])
        for group, data in ad_group_grouped:
            sku = data['SKU'][data['Record Type'] == 'Ad'].values[0]
            data['SKU'].fillna(value=sku, inplace=True)
            one_group_data.append(data)
        camp_data_one_to_one = pd.concat(one_group_data)

        return camp_data_one_to_one

    def new_st_upload_format(station_name, st_n_camp_data, camp_data, active_listing_data, init_acos):
        """
        描述:
            st 新增
        逻辑：
            对st和camp合并后的数据进行按照acos和出单的情况进行分类上传
            按照camp name /ad group/sku分类汇总，得到sku下的全部kws的bid
        :param station_name:
        :param st_data:
        :return:
        """
        export_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                          'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword',
                          'Product Targeting ID',
                          'Match Type', 'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']

        station_name = station_name.upper()
        site = station_name[-2:]
        bid_exchange_rate = bid_exchange[site]
        bid_exchange_unit = 0.01 / bid_exchange_rate

        # 生成用于去重的辅助列 (优化速度 将一对多的广告组单独处理)
        camp_data_temp = camp_data.copy()
        camp_data_temp['ad_group_temp'] = camp_data_temp['Campaign'] + camp_data_temp['Ad Group']
        camp_ad_group_temp_grouped = camp_data_temp.groupby(['ad_group_temp'])
        ad_group_one_to_more_sku = []
        ad_group_one_to_one_sku = []
        for ad_group_temp_name, grouped_data in camp_ad_group_temp_grouped:
            sku_num = [sku for sku in grouped_data['SKU'] if pd.notnull(sku)]
            if len(sku_num) > 1:
                ad_group_one_to_more_sku.append(ad_group_temp_name)
            if len(sku_num) == 1:
                ad_group_one_to_one_sku.append(ad_group_temp_name)
        # ad group对应一个sku
        ad_group_one_to_one_sku_data = camp_data_temp[camp_data_temp['ad_group_temp'].isin(ad_group_one_to_one_sku)]
        ad_group_one_to_one_sku_data['SKU'].fillna(method='ffill', inplace=True)
        ad_group_one_to_one_sku_data_kws_degree = ad_group_one_to_one_sku_data[
            ad_group_one_to_one_sku_data['Record Type'].isin(['Product Targeting', 'Keyword'])]
        ad_group_one_to_one_sku_kws_sign = set(ad_group_one_to_one_sku_data_kws_degree['SKU'] +
                                               ad_group_one_to_one_sku_data_kws_degree['Keyword or Product Targeting'] +
                                               ad_group_one_to_one_sku_data_kws_degree['Match Type'])
        # ad group对应多个sku
        ad_group_one_to_more_sku_data = camp_data_temp[camp_data_temp['ad_group_temp'].isin(ad_group_one_to_more_sku)]

        ad_group_set = set(ad_group_one_to_more_sku_data['ad_group_temp'].values)
        all_kws_repeat_sign = []
        for ad_group in ad_group_set:
            if pd.isnull(ad_group):
                continue
            one_ad_group_data = ad_group_one_to_more_sku_data[
                ad_group_one_to_more_sku_data['ad_group_temp'] == ad_group]
            one_ad_group_sku = set(one_ad_group_data['SKU'].values)
            one_ad_group_sku = [sku for sku in one_ad_group_sku if pd.notnull(sku)]
            if len(one_ad_group_sku) == 0:
                continue
            one_ad_group_kws = [kw for kw in one_ad_group_data['Keyword or Product Targeting'] if pd.notnull(kw)]
            one_ad_group_match_type = [match_type for match_type in one_ad_group_data['Match Type'] if
                                       pd.notnull(match_type)]
            if len(one_ad_group_sku) == 1:
                one_ad_group_repeat = [f'{one_ad_group_sku[0]}{kw}{match_type}' for kw, match_type in
                                       zip(one_ad_group_kws, one_ad_group_match_type)]
            else:
                one_ad_group_repeat = []
                for one_sku in one_ad_group_sku:
                    if pd.isna(one_sku):
                        continue
                    one_sku_repeat = []
                    for kw, match_type in zip(one_ad_group_kws, one_ad_group_match_type):
                        one_kw_repeat_sign = f'{one_sku}{kw}{match_type}'
                        one_sku_repeat.append(one_kw_repeat_sign)
                    one_ad_group_repeat.extend(one_sku_repeat)
            all_kws_repeat_sign.extend(one_ad_group_repeat)
        ad_group_one_to_more_sku_kws_sign = set(all_kws_repeat_sign)
        camp_data_temp_set = ad_group_one_to_more_sku_kws_sign | ad_group_one_to_one_sku_kws_sign

        # 按照camp name /ad group/sku汇总
        # 计算广泛广告
        #  这里不能用camp name去分组，可以用Match Type_x来分组
        # 先处理ST报表中的关键词新增
        new_keyword_data = st_n_camp_data[~st_n_camp_data['Customer Search Term'].str.contains('b0', case=False)]
        new_asin_data = st_n_camp_data[st_n_camp_data['Customer Search Term'].str.contains('b0', case=False)]

        # A.关键词新增聚合
        grouped_data_keyword = new_keyword_data.groupby(['Ad Group Name', 'SKU', 'Match Type_x'])
        # B.ASIN新增聚合
        grouped_data_asin = new_asin_data.groupby(['Ad Group Name', 'SKU', 'Match Type_x'])

        def calc_one_sku(one_grouped_data, active_listing_data, init_acos, match_type='broad'):
            empty_list = [np.nan] * len(export_columns)
            sku_name = one_grouped_data['SKU'].values[0]

            # 判断SKU是否在售
            asin = active_listing_data['asin1'][active_listing_data['seller-sku'] == sku_name]
            if len(asin) > 0:
                asin = asin.values[0]
                ad_group_name = f'{sku_name} {asin}'
            else:
                return pd.DataFrame(columns=export_columns)
            kws = set(one_grouped_data['Customer Search Term'])
            bid_list = []
            not_repeat_kw = []
            # 给sku_bid
            sku_bid = ad_group_least_bid[site]
            for kw in kws:
                if match_type == 'broad':
                    repeat_detect = sku_name + kw + 'broad'
                else:
                    repeat_detect = sku_name + kw + 'exact'
                # 去掉重复广告
                if repeat_detect in camp_data_temp_set:
                    continue
                kw_acos = one_grouped_data['Advertising Cost of Sales (ACoS)'][
                    one_grouped_data['Customer Search Term'] == kw].values[0]
                if isinstance(kw_acos, str):
                    if '%' in kw_acos:
                        kw_acos = float(kw_acos.replace('%', '')) / 100
                    else:
                        kw_acos = float(kw_acos)
                order = \
                    one_grouped_data['7 Day Total Orders (#)'][one_grouped_data['Customer Search Term'] == kw].values[0]
                if isinstance(order, str):
                    order = int(order)
                cpc = one_grouped_data['Cost Per Click (CPC)'][one_grouped_data['Customer Search Term'] == kw].values[0]
                if isinstance(cpc, str):
                    cpc = float(cpc)
                bid_kw = one_grouped_data['Targeting'][one_grouped_data['Customer Search Term'] == kw].values[0]
                targeting_kw = one_grouped_data['Max Bid'][one_grouped_data['Keyword or Product Targeting'] == bid_kw]
                if len(targeting_kw) > 0:
                    ad_group_bid = \
                        one_grouped_data['Max Bid'][one_grouped_data['Keyword or Product Targeting'] == bid_kw].values[
                            0]
                    group_type = one_grouped_data['Campaign Targeting Type'].values[0]
                    if group_type.lower() == 'manual':
                        camp_type = 'manual'
                    else:
                        camp_type = 'auto'
                else:
                    ad_group_bid = one_grouped_data['Max Bid'].values[0]
                    camp_type = 'auto'
                if isinstance(ad_group_bid, str):
                    ad_group_bid = currency_trans(ad_group_bid)
                if pd.isnull(cpc):
                    cpc = one_grouped_data['Max Bid'].values[0]
                # logic
                if match_type == 'exact':
                    add_bid = bid_exchange_unit
                else:
                    add_bid = 0
                if kw_acos < init_acos * 0.1:
                    if order == 1:
                        if camp_type == 'manual':
                            bid = ad_group_bid + 2 * bid_exchange_unit
                        else:
                            bid = cpc + 2 * bid_exchange_unit
                    elif (order > 1) & (order < 5):
                        bid = ad_group_bid + 3 * bid_exchange_unit
                    else:
                        bid = ad_group_bid + 4 * bid_exchange_unit
                elif kw_acos < init_acos * 0.3:
                    if order == 1:
                        if camp_type == 'manual':
                            bid = ad_group_bid + bid_exchange_unit
                        else:
                            bid = cpc + bid_exchange_unit
                    elif (order > 1) & (order < 5):
                        bid = ad_group_bid + bid_exchange_unit
                    else:
                        bid = ad_group_bid + 2 * bid_exchange_unit
                elif kw_acos <= init_acos:
                    if order == 1:
                        if camp_type == 'manual':
                            bid = ad_group_bid
                        else:
                            bid = cpc
                    elif (order > 1) & (order < 5):
                        bid = ad_group_bid
                    else:
                        bid = ad_group_bid + bid_exchange_unit
                elif kw_acos > init_acos:
                    bid = cpc * (init_acos / kw_acos)
                # excat 加上0.01
                bid += add_bid
                if site == 'JP':
                    bid = int(bid)
                else:
                    bid = round(bid, 2)
                bid_list.append(bid)
                not_repeat_kw.append(kw)

            if not bid_list:
                return pd.DataFrame(columns=export_columns)
            data_len = len(bid_list)
            row = data_len + 2
            processed_st_new_data = pd.DataFrame([empty_list] * row, columns=export_columns)
            if match_type == 'broad':
                processed_st_new_data['Campaign Name'] = f'MANUAL-{station_name}-by-SP_Bulk'
            elif match_type == 'exact':
                processed_st_new_data['Campaign Name'] = f'MANUAL-ST-EXACT-by-SP_Bulk'
            else:
                print(f'ST match_type: {station_name}站点的匹配方式不对...')

            processed_st_new_data.loc[1, 'SKU'] = sku_name
            # 重新给ad group name 判断ad group 中有没有asin
            processed_st_new_data['Ad Group Name'] = ad_group_name
            processed_st_new_data.loc[0, 'Max Bid'] = sku_bid
            processed_st_new_data['Campaign Status'] = 'enabled'
            processed_st_new_data['Ad Group Status'] = 'enabled'
            processed_st_new_data.loc[1:, 'Status'] = 'enabled'
            processed_st_new_data.loc[2:, 'Keyword'] = not_repeat_kw
            processed_st_new_data.loc[2:, 'Max Bid'] = bid_list
            processed_st_new_data.loc[2:, 'Match Type'] = match_type

            return processed_st_new_data

        # 1.计算关键词新增中的广泛出价广告
        broad_data = [calc_one_sku(one_grouped_data, active_listing_data, init_acos) for sku_index, one_grouped_data in
                      grouped_data_keyword]
        broad_data = pd.concat(broad_data)

        # 第一行
        camp_name = f'MANUAL-{station_name}-by-SP_Bulk'
        if site in ['CA', 'DE', 'FR', 'IT', 'SP', 'UK', 'US', 'ES']:
            daily_budge = 200
        else:
            daily_budge = int(200 / bid_exchange_rate)
        broad_data_first_row = pd.DataFrame(
            [[camp_name, daily_budge, None, None, 'Manual', None, None, None, None, None,
              None, 'enabled', None, None, 'Dynamic bidding (down only)']], columns=export_columns)
        broad_data = pd.concat([broad_data_first_row, broad_data])

        # 2.计算关键词新增中的精准出价广告
        exact_data = [calc_one_sku(one_grouped_data, active_listing_data, init_acos, match_type='exact') for
                      sku_index, one_grouped_data in
                      grouped_data_keyword]
        exact_data = pd.concat(exact_data)

        # 第一行
        exact_camp_name = 'MANUAL-ST-EXACT-by-SP_Bulk'
        if site in ['CA', 'DE', 'FR', 'IT', 'SP', 'UK', 'US', 'ES']:
            daily_budge = 200
        else:
            daily_budge = int(200 / bid_exchange_rate)
        exact_data_first_row = pd.DataFrame(
            [[exact_camp_name, daily_budge, None, None, 'Manual', None, None, None, None, None,
              None, 'enabled', None, None, 'Dynamic bidding (down only)']], columns=export_columns)
        exact_data = pd.concat([exact_data_first_row, exact_data])

        # 3.计算ASIN
        asin_data = [calc_one_sku(one_grouped_data, active_listing_data, init_acos, match_type='broad') for
                     sku_index, one_grouped_data in
                     grouped_data_asin]
        asin_data = pd.concat(asin_data)
        # 计算ASIN的第一行
        asin_camp_name = 'Negative Targeting Expression-SP_Bulk'
        if site in ['CA', 'DE', 'FR', 'IT', 'SP', 'UK', 'US', 'ES']:
            daily_budge = 200
        else:
            daily_budge = int(200 / bid_exchange_rate)
        asin_data_first_row = pd.DataFrame(
            [[asin_camp_name, daily_budge, None, None, 'Manual', None, None, None, None, None,
              None, 'enabled', None, None, 'Dynamic bidding (down only)']], columns=export_columns)
        asin_data = pd.concat([asin_data_first_row, asin_data])
        asin_data['Keyword'] = asin_data['Keyword'].fillna(value='')
        # # 修改ASIN新增中的Campaign Name,Keyword,Product Targeting ID和Match Type四列
        # 否定ASIN的Campaign Name是固定写法"Negative Targeting Expression-SP_Bulk"
        asin_campaign_name = "Negative Targeting Expression-SP_Bulk"
        asin_data['Campaign Name'] = asin_campaign_name
        asin_data['Keyword'] = [f'asin="{asin.upper()}"' if (pd.notnull(asin) & (asin not in ['', ' '])) else asin for
                                asin in asin_data['Keyword']]
        asin_data['Product Targeting ID'] = asin_data['Keyword']
        asin_data['Match Type'] = ['Targeting Expression' if (pd.notnull(asin) & (asin not in ['', ' '])) else asin for
                                   asin in asin_data['Keyword']]

        all_st_data = pd.concat([broad_data, exact_data, asin_data])
        if all_st_data.empty:
            print(f'st 新增为空: {station_name}')
            return
        all_st_data = all_st_data[export_columns]

        return all_st_data

    def get_init_acos(station_name, camp_ori_data):
        """
        描述:
            通过广告报表中acos和销售额的表现,得到用于计算ST新增中的指定acos
        逻辑:
            1.acos>15% : init_acos = 15%
            2.acos<15%
                1. sales > 1000美元 : init_acos = acos - 1%
                2. sales < 1000美元
                    1. acos > 11% :init_acos = acos - 1%
                    1. acos < 11% :init_acos = 10%
        :param station_name:站点名
        :param camp_ori_data:站点的广告报表原始数据
        :return:指定的acos(init_acos)
        """
        # 1.判断st是否有效
        if (camp_ori_data is None) or (not isinstance(camp_ori_data, pd.DataFrame)) or (camp_ori_data.empty):
            print(f'camp data error: {station_name}')
            return
        # 2.判断列是否存在
        columns = camp_ori_data.columns
        need_columns = {'Spend', 'Sales'}
        if not need_columns.issubset(columns):
            print(f'lost columns:{station_name} 的camp表缺少 {need_columns - columns}')
            return
        site = station_name[-2:].upper()
        for column in ['Spend', 'Sales']:
            if camp_ori_data[column].dtype not in [np.float64, np.int64]:
                camp_ori_data[column] = camp_ori_data[column].apply(lambda x: currency_trans(x))
        # camp表中包含五个层级的数据，于是需要除以5
        station_spend = sum(camp_ori_data['Spend']) * sale_exchange_rate[site] / 5
        station_sales = sum(camp_ori_data['Sales']) * sale_exchange_rate[site] / 5
        station_acos = station_spend / station_sales

        # 逻辑
        if station_acos > 0.15:
            return 0.15
        elif station_sales > 1000:
            return station_acos - 0.01
        elif station_acos > 0.11:
            return station_acos - 0.01
        else:
            return 0.1

    # 1.判断st和camp数据的有效性
    result = detect_process_data_error(st_data, camp_data)
    if not result:
        return
    # 2.初始化st 和camp
    new_st_data = init_st_data(st_data)
    new_camp_data = get_one_to_one_camp_data(camp_data)
    # 3.st表和camp表通过 camp ad group来连接
    st_n_camp_data = pd.merge(new_st_data, new_camp_data[
        ['Campaign', 'Ad Group', 'Max Bid', 'Keyword or Product Targeting', 'Match Type', 'SKU',
         'Campaign Targeting Type']],
                              left_on=['Campaign Name', 'Ad Group Name'],
                              right_on=['Campaign', 'Ad Group'], how='right')
    st_n_camp_data = st_n_camp_data[pd.notnull(st_n_camp_data['Campaign Name'])]
    # 4.ST自动新增
    init_acos = get_init_acos(station_name, camp_data)
    upload_data = new_st_upload_format(station_name, st_n_camp_data, camp_data, active_listing_data, init_acos)

    return upload_data


# 货币转换
def currency_trans(currency) -> 'digit':
    """
    将货币装换成数字
    逻辑:
        通过判断倒数第三位是否是,(逗号)或是.(点号)来判断是小数还是整数
    :param currency:需要转换的货币
    :return: 整型或浮点型货币
    """
    if pd.isnull(currency):
        return
    if not isinstance(currency, str):
        return
    else:
        currency = currency.strip(' ')
        currency_temp = re.findall('\d.*', currency)
        if len(currency_temp) == 1:
            currency_temp = currency_temp[-1]
            if currency_temp[-3] in [',', '.']:
                # 该数字为包含两位小数的数字
                return float(re.sub('[,.]', '', currency_temp)) / 100
            else:
                # 该数字不包含两位小数的数字
                return int(re.sub('[,.]', '', currency_temp))
        if not currency_temp:
            return
        if len(currency_temp) > 1:
            return


# 处理队列
class Queue(object):
    # 定义一个空队列
    def __init__(self):
        self.items = []

    # 队列(只能在队尾)添加一个元素
    def enqueue(self, item):
        self.items.append(item)

    # 删除队列（只能在对头）一个元素
    def dequeue(self):
        self.items.pop(0)

    # 判断队列是否为空
    def isEmpty(self):
        return (self.items == [])

    # 清空队列
    def clear(self):
        del (self.items)  # 该队列就不存在了，而不是清空元素

    # 返回队列项的数量
    def size(self):
        return (len(self.items))

    # 打印队列
    def print(self):
        print(self.items)


# 加载全部的站点名和广告专员
def db_download_station_names(db='team_station', table='only_station_info', ip='wuhan.yibai-it.com', port=33061,
                              user_name='marmot', password='') -> pd.DataFrame:
    """
    加载广告组接手的站点名
    :return: 所有站点名
    """
    conn = pymysql.connect(
        host=ip,
        user=user_name,
        password=password,
        database=db,
        port=port,
        charset='UTF8')
    # 创建游标
    cursor = conn.cursor()
    # 写sql
    sql = """SELECT station,ad_manger FROM {} """.format(table)
    # 执行sql语句
    cursor.execute(sql)
    stations_name_n_manger = cursor.fetchall()
    stations_name_n_manger = pd.DataFrame([list(station) for station in stations_name_n_manger],
                                          columns=['station_name', 'manger'])
    stations_name_n_manger.drop_duplicates(inplace=True)
    conn.commit()
    cursor.close()
    conn.close()
    return stations_name_n_manger


# 获取站点文件的请求路径
def get_all_files_dir(station_name, download_url="http://120.78.243.154/services/api/advertise/getreport"):
    key_path = f"{file_load_drive}:/api_request_all_files/public.key"
    with open(key_path, 'r') as fp:
        public_key = fp.read()
    # pkcs8格式
    key = public_key
    password = "Kr51wGeDyBM39Q0REVkXn4lW7ZqCxdPLS8NO6iIfubTJcvsjt2YpmAgzHFUoah"
    pubkey = rsa.PublicKey.load_pkcs1_openssl_pem(key)
    password = password.encode('utf8')
    crypt_password = rsa.encrypt(password, pubkey)
    token = base64.b64encode(crypt_password).decode()
    station_name = station_name[0:-3].replace('_', '-') + station_name[-3:]

    def get_report(station_name):
        post_load = {
            'token': token,
            'data': json.dumps({
                0: {
                    'date_range': 1,
                    "child_type": 30,
                    'account_id': station_name,
                    "report_type": "Campaign"
                },
                1: {
                    'date_range': 1,
                    'account_id': station_name,
                    "report_type": "ST"
                },
                2: {
                    'date_range': 1,
                    'account_id': station_name,
                    "report_type": "BR"
                },
                3: {
                    'date_range': 30,
                    "child_type": 30,
                    "account_id": station_name,
                    "report_type": "AO"
                },
                4: {
                    "account_id": station_name,
                    "report_type": "Active"
                },
                5: {
                    "account_id": station_name,
                    "report_type": "All"
                },
            })
        }
        response = requests.post(download_url, data=post_load).content
        data = []
        try:
            data_basic = json.loads(response)['data']
            data.extend(data_basic)
        except:
            pass
            # red.rpush('station_no_data', station_name)
            # return
        # 单独请求四天的广告报表
        post_camp_date = [1, 7, 14, 60]
        all_camp_post_dir = []
        for post_date in post_camp_date:
            post_camp_load = {
                'token': token,
                'data': json.dumps({
                    0: {
                        'date_range': 1,
                        "child_type": post_date,
                        'account_id': station_name,
                        "report_type": "Campaign"
                    }
                })
            }
            response_camp = requests.post(download_url, data=post_camp_load).content
            try:
                camp_data = json.loads(response_camp)['data']
            except:
                print(f"{station_name}广告报表{post_date}无法请求..")
                continue
            all_camp_post_dir.extend(camp_data)
        # data = all_camp_post_dir

        # 本地接口请求AO
        '''
        local_url = 'http://192.168.9.167:8080/services/api/advertise/getreport'
        post_ao_load = {
            'token': token,
            'data': json.dumps({
                0: {
                    'date_range': 30,
                    "child_type": 30,
                    'account_id': station_name,
                    "report_type": "AO"
                }
            })
        }
        response_ao = requests.post(local_url,data=post_ao_load).content
        try:
            ao_data = json.loads(response_ao)['data']
            ao_data = [data.replace('D:/phpStudy/PHPTutorial/WWW/wwwerp','http://192.168.9.167:8080') for data in ao_data]
        except:
            print(f"{station_name}:AO报表{post_date}本地无法请求..")
            ao_data = []
        data.extend(ao_data)
        '''

        data.extend(all_camp_post_dir)
        return data

    data = get_report(station_name)
    if (not data) & (station_name[-2:] == 'es'):
        station_name = station_name[:-2] + 'sp'
        data = get_report(station_name)
    if not data:
        station_name = station_name[0:-3].replace('-', ' ') + station_name[-3:]
        data = get_report(station_name)

    # print(data)
    if not data:
        return
    files_keyword_dict = {'ST': 'SearchTerm', 'BR': 'Business', 'AO': 'ORDER', 'AC': 'AVTIVE_LISTING',
                          'AL': 'All_LISTING'}
    camp_keyword_dict = ['Advertising', 'Sponsored']
    all_files_dict = {}
    for report_type, report_kw in files_keyword_dict.items():
        all_files_dict[report_type] = [report for report in data if files_keyword_dict[report_type] in report]
    all_files_dict['CP'] = [report for report in data if
                            (camp_keyword_dict[0] in report) or (camp_keyword_dict[1] in report)]
    return all_files_dict


def keep_newest_file_dir(all_files_dict: 'dict', station_name):
    file_keys = all_files_dict.keys()
    for report_type in file_keys:
        report_type_files = all_files_dict[report_type]
        if report_type == 'ST':
            continue
        if len(report_type_files) > 1:
            try:
                files_date = [re.findall('[0-9]{4}.[0-9]{2}.[0-9]{2}', os.path.basename(file)) for file in
                              report_type_files]
                # 排除没有日期的链接
                if not files_date[0]:
                    continue
                last_date = max([max(dates) for dates in files_date])
                all_files_dict[report_type] = [file for file in report_type_files if
                                               last_date in os.path.basename(file)]
            except:
                print(f"{station_name}有文件命名有问题.")
                pass
    return all_files_dict


# 得到站点文件请求情况.
def request_log(station_folder):
    files_match_kw_dict = {'camp_30': '30天-bulksheet', 'order': 'order', 'br': 'business',
                           'st': 'search', 'al': 'all+listing', 'ac': 'active+listing', 'camp_1': '1天-bulksheet',
                           'camp_7': '7天-bulksheet', 'camp_14': '14天-bulksheet', 'camp_60': '60天-bulksheet'}
    requested_files = os.listdir(station_folder)
    stations_name = os.path.basename(station_folder)
    file_result_columns = ['站点名', '完整情况', '缺失文件数量']
    file_result_columns.extend(files_match_kw_dict.keys())
    station_file_result = pd.Series([''] * len(file_result_columns), index=file_result_columns)
    station_file_result['站点名'] = stations_name
    for file_type in files_match_kw_dict.keys():
        matched_file = [file for file in requested_files if files_match_kw_dict[file_type] in file.lower()]
        if matched_file:
            station_file_result[file_type] = '存在'
        else:
            station_file_result[file_type] = '不存在'
    lost_num = len(station_file_result[station_file_result == '不存在'])
    if lost_num == 0:
        station_file_result['完整情况'] = '完整'
    else:
        station_file_result['完整情况'] = '不完整'
    station_file_result['缺失文件数量'] = lost_num
    station_file_result = list(station_file_result)
    return station_file_result


# 读取单个文件数据(若为excel,则读取单个sheet)
def read_files(files_path: 'full_path', sheet_name='Sheet1'):
    split_file_path = os.path.splitext(files_path)
    if len(split_file_path) > 1:
        file_type = split_file_path[-1].lower()
        if file_type in ['.csv', '.txt']:
            try:
                file_data = pd.read_csv(files_path, error_bad_lines=False, warn_bad_lines=False)
                return file_data
            except Exception as e:
                file_data = pd.read_csv(files_path, encoding="ISO-8859-1", error_bad_lines=False, warn_bad_lines=False)
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


# 请求并保存请求到的6种类型的报表
def request_save_all_6_files():
    files_save_dirname = f"{file_load_drive}:/api_request_all_files"
    now_date = str(datetime.now().date())
    files_save_dirname = os.path.join(files_save_dirname, now_date)
    if stations_queue.size() == 0:
        return
    station_name = stations_queue.items[0]
    try:
        stations_queue.dequeue()
        print(station_name)
    except Exception as e:
        print(station_name)
        return
    print(stations_queue.size())
    if station_name:
        # print(f"START: {station_name} ")
        all_file_dict = get_all_files_dir(station_name)
        if not all_file_dict:
            return
        all_file_key = all_file_dict.keys()
        all_file_dict = keep_newest_file_dir(all_file_dict, station_name)

        station_save_folder = os.path.join(files_save_dirname, station_name)
        if os.path.exists(station_save_folder):
            shutil.rmtree(station_save_folder)

        # 规范命名
        def unified_reports_name(files_folder: 'dirname', station_name) -> dict:
            '''
            广告报表    :账号-国家-30（7/14/30/60）天-bulksheet-月-日-年
            搜索词报告  :Sponsored Products Search term report-月-日-年
            业务报告    :BusinessReport-月-日-年
            在售商品报告:Active+Listings+Report+月-日-年
            全部商品报告:All+Listings+Report+月-日-年
            订单报告    :All Orders-月-日-年
            :param all_file_dict:
            :return:
            '''
            try:
                all_report_files = os.listdir(files_folder)
            except:
                return
            account = station_name[:-3]
            site = station_name[-2:]
            date = datetime.now().strftime('%m-%d-%Y')
            if not all_report_files:
                return
            report_sign_word = {'sevendays': 7, 'fourteendays': 14, 'bulknearlyamonth': 30, 'sixtydays': 60,
                                'amazonsponsoredproductsbulk': 1, 'amazonsearchtermreportmonthtodate': '(last_month)',
                                'amazonsearchtermreport': '', 'business': '', 'all_listing': '', 'avtive_listing': '',
                                'order': ''}
            # 广告报表改名字典
            report_sign_word = {key: f'{account}-{site}-{value}天-bulksheet-{date}' if ('day' in key.lower()) or (
                    'bulk' in key.lower()) else value for key, value in
                                report_sign_word.items()}
            # 搜索词改名字典
            report_sign_word = {
                key: f'Sponsored Products Search term report{value}-{date}' if ('search' in key.lower()) else value for
                key, value in
                report_sign_word.items()}
            # 业务报表改名字典
            report_sign_word = {key: f'BusinessReport-{date}' if
            'business' in key.lower() else value for key, value in report_sign_word.items()}
            # 在售商品改名字典
            report_sign_word = {key: f'Active+Listings+Report+{date}' if
            'avtive_listing' in key.lower() else value for key, value in report_sign_word.items()}

            # 全部商品改名字典
            report_sign_word = {key: f'All+Listings+Report+{date}' if
            'all_listing' in key.lower() else value for key, value in report_sign_word.items()}
            # 订单报告改名字典
            nine_foundation_num = int(1e8)
            random_num = random.randint(1 * nine_foundation_num, 9 * nine_foundation_num)
            order_data = datetime.now().strftime('%Y%m%d')
            report_sign_word = {key: f'allorders{str(random_num)}{order_data}' if
            'order' in key.lower() else value for key, value in report_sign_word.items()}

            for file in all_report_files:
                for key in report_sign_word.keys():
                    if key in file.lower():
                        new_file_dirname = report_sign_word[key]
                        file_type = os.path.splitext(file)[-1]
                        try:
                            os.rename(os.path.join(files_folder, file),
                                      os.path.join(files_folder, new_file_dirname + file_type))
                            break
                        except:
                            break

        # api请求报表
        def download_from_api(api_dir: 'dir', files_save_dirname, station_name, file_type):
            newest_dir = api_dir
            newest_dir = newest_dir.replace('/mnt/erp', 'http://120.78.243.154')
            file_basename = os.path.basename(newest_dir)
            # 除了Br表,其他表都将CSV格式转换为txt格式
            if (file_type != 'BR') & ('.csv' in file_basename.lower()):
                file_basename = file_basename.lower().replace('csv', 'txt')
            try:
                request_file = requests.get(newest_dir)
            except Exception as e:
                print(e)
                print(f'{station_name}: 请求的链接{newest_dir}')
                return
            status_code = request_file.status_code
            if status_code == 200:
                out_content = request_file.content
                # 读入到内存中
                # rawData = pd.read_csv(io.StringIO(out_content.decode('utf-8')))
                file_dirname = os.path.join(files_save_dirname, station_name)
                if not os.path.exists(file_dirname):
                    os.makedirs(file_dirname)
                files_save_dirname = os.path.join(file_dirname, file_basename)
                # print(file_dirname)
                with open(files_save_dirname, 'wb') as f:
                    f.write(out_content)
                    if file_type != 'ST':
                        if file_type == 'AC':
                            ac_data = change_file_columns_name(file_type, files_save_dirname)
                            return ac_data
                        elif file_type == 'AO':
                            ao_data = change_file_columns_name(file_type, files_save_dirname)
                            return ao_data
                        else:
                            change_file_columns_name(file_type, files_save_dirname)
            else:
                if 'MonthToDate' in newest_dir:
                    return
                print(f'无法请求{newest_dir}报表! \n status_code:{status_code}')

        # 修改erp请求的文件中的一些问题，方便小程序使用
        def change_file_columns_name(file_type, file_path):
            # ac all_listing表处理列名对齐，发货方式，列名的修改
            if file_type in ['AC', 'AL']:
                ac_data = read_files(file_path)
                if not ac_data.empty:
                    ac_data.columns = [column.strip(" ").lower() for column in ac_data.columns]
                    ac_change_columns = {'fulfillment_channel': 'fulfillment-channel', 'seller_sku': 'seller-sku',
                                         'open_date': 'open-date', "item_name": "item-name"}
                    ac_data.rename(columns=ac_change_columns, inplace=True)
                    if 'fulfillment-channel' in ac_data.columns:
                        ac_data['fulfillment-channel'].replace('DEF', 'DEFAULT', inplace=True)
                        ac_data['fulfillment-channel'].replace('AMA', 'AMAZON', inplace=True)
                    ac_data.to_csv(file_path, sep='\t', encoding="UTF-8", index=False)
                if file_type == 'AC':
                    return ac_data
            # ao表处理列名的修改
            if file_type == 'AO':
                ao_data = read_files(file_path)
                ao_data.columns = [column.strip(" ").replace("_", '-') for column in ao_data.columns]
                if 'fulfillment-channel' in ao_data.columns:
                    ao_data['fulfillment-channel'].replace('MFN', 'Merchant', inplace=True)
                    ao_data['fulfillment-channel'].replace('AFN', 'Amazon', inplace=True)
                ao_data.to_csv(file_path, sep='\t', encoding="UTF-8", index=False)
                return ao_data

        # 搜索词报表的合并
        def combine_st(file_folder, station_name):
            """
            目的: 将上月的搜索词报表和本月至今的搜索报表合并
            逻辑: 将相同的广告大组/广告组/匹配方式/搜索词合并,对相应的计算项进行处理,另外起始时间分别为最小和最大。
            步骤: 1.对搜索词报表进行预处理
                  2.按照逻辑进行合并
            :param fileFolder:全部站点存储的文件夹
            :param stationName:站点名
            :return:合并后的搜索词报表
            """
            # STEP1 预处理
            station_folder = os.path.join(file_folder, station_name)
            if not os.path.exists(station_folder):
                return
            station_files = os.listdir(station_folder)
            st_keyword = 'search'
            st_reports_name = [file for file in station_files if st_keyword in file.lower()]
            if len(st_reports_name) != 2:
                return
            # STEP2 对ST做初始化处理
            std_st_columns = ['Start Date', 'End Date', 'Portfolio name', 'Currency', 'Campaign Name', 'Ad Group Name',
                              'Targeting', 'Match Type',
                              'Customer Search Term', 'Impressions', 'Clicks', 'Click-Thru Rate (CTR)',
                              'Cost Per Click (CPC)', 'Spend', '7 Day Total Sales',
                              'Advertising Cost of Sales (ACoS)', 'Return on Advertising Spend (RoAS)',
                              '7 Day Total Orders',
                              '7 Day Total Units', '7 Day Conversion Rate',
                              '7 Day Advertised SKU Units', '7 Day Other SKU Units', '7 Day Advertised SKU Sales',
                              '7 Day Other SKU Sales']
            combine_st_data = pd.DataFrame()
            for file in st_reports_name:
                st_file_path = os.path.join(station_folder, file)
                st_file_data = read_files(st_file_path)
                os.remove(st_file_path)
                if st_file_data.empty:
                    return
                '''
                columns_name = [
                    col.replace('($)', '').replace('(£)', '').replace('(￥)', '').replace('(₹)', '').replace('(#)',
                                                                                                            '').strip(
                        " ").strip('Total') for col
                    in st_file_data.columns]
                if columns_name != std_st_columns:
                    return
                '''
                # 判断列名的相似性
                columns_name = st_file_data.columns
                if len(columns_name) != len(std_st_columns):
                    lose_columns = set(std_st_columns) - set(columns_name)
                    print(f'{station_name}: ST表缺失列{lose_columns}。')
                    return
                unmatch_columns = [col + '与' + std_col + '不匹配' for col, std_col in zip(columns_name, std_st_columns) if
                                   difflib.SequenceMatcher(None, columns_name, std_st_columns).quick_ratio() < 0.4]
                if unmatch_columns:
                    print(f'{station_name}: ST表列不匹配{unmatch_columns}。')
                    return

                st_file_data.columns = std_st_columns
                int_columns_keyword = ['impression', 'clicks', 'order', 'unit']
                float_columns_keyword = ['cpc', 'spend', 'sale']
                int_columns = [col for col in std_st_columns for keyword in int_columns_keyword if
                               keyword in col.lower()]
                float_columns = [col for col in std_st_columns for keyword in float_columns_keyword if
                                 keyword in col.lower()]
                for col in int_columns:
                    if st_file_data[col].dtypes != int:
                        try:
                            st_file_data[col] = st_file_data[col].apply(lambda x: int(x))
                        except Exception as e:
                            print(f'{station_name} :ST表的{col}列不能识别成整型.')
                            return
                for col in float_columns:
                    if st_file_data[col].dtypes != float:
                        try:
                            st_file_data[col] = st_file_data[col].apply(
                                lambda x: int(re.sub('[^0-9]', '', str(x))) / 100 if '.' in str(x) else round(
                                    float(re.sub('[^0-9]', '', str(x)))), 2)
                        except Exception as e:
                            print(f'{station_name} :ST表的{col}列不能识别成浮点型.')
                            return
                combine_st_data = combine_st_data.append(st_file_data)
            # STEP3 对st表进行合并
            combine_st_data = combine_st_data.groupby(
                ['Campaign Name', 'Ad Group Name', 'Match Type', 'Targeting', 'Customer Search Term']).agg(
                {'Start Date': 'min', 'End Date': 'max', 'Portfolio name': 'first', 'Currency': 'first'
                    , 'Impressions': 'sum', 'Clicks': 'sum', 'Spend': 'sum',
                 '7 Day Total Sales': 'sum', '7 Day Total Orders': 'sum', '7 Day Total Units': 'sum',
                 '7 Day Conversion Rate': 'last',
                 '7 Day Advertised SKU Units': 'sum', '7 Day Other SKU Units': 'sum',
                 '7 Day Advertised SKU Sales': 'sum', '7 Day Other SKU Sales': 'sum',
                 'Return on Advertising Spend (RoAS)': 'sum'}).reset_index()
            # STEP4 生成ST合并表中的比例列以及各式
            rate_columns = {'ctr': 'Click-Thru Rate (CTR)', 'cpc': 'Cost Per Click (CPC)',
                            'acos': 'Advertising Cost of Sales (ACoS)'}

            def calc_format_rate(numerator_list: "up/smaller", denominator_list: 'down/biger', point_keep=4):
                rate_list = [str(round(numerator * 100 / denominator,
                                       point_keep)) + '%' if denominator > 0 else '0.' + '0' * point_keep + '%'
                             for numerator, denominator in zip(numerator_list, denominator_list)]
                return rate_list

            ctr = rate_columns['ctr']
            combine_st_data[ctr] = calc_format_rate(combine_st_data['Clicks'], combine_st_data['Impressions'])
            acos = rate_columns['acos']
            combine_st_data[acos] = calc_format_rate(combine_st_data['Spend'], combine_st_data['7 Day Total Sales'])
            '''
            cr = rate_columns['cr']
            combine_st_data[cr] = calc_format_rate(combine_st_data['Clicks'], combine_st_data['7 Day Total Orders'])
            '''
            cpc = rate_columns['cpc']
            combine_st_data[cpc] = [round(numerator / denominator,
                                          2) if denominator > 0 else '0.' + '0' * 2
                                    for numerator, denominator in
                                    zip(combine_st_data['Spend'], combine_st_data['Clicks'])]
            combine_st_data = combine_st_data[std_st_columns]
            save_combined_st_dirname = \
                [file_dirname for file_dirname in st_reports_name if 'last_month' not in file_dirname][0]
            save_combined_st_path = os.path.join(station_folder, save_combined_st_dirname)
            # 修改列名，方便小程序识别
            change_columns = {'7 Day Total Orders': '7 Day Total Orders (#)',
                              '7 Day Advertised SKU Units': '7 Day Advertised SKU Units (#)',
                              '7 Day Total Units': '7 Day Total Units (#)',
                              '7 Day Other SKU Units': '7 Day Other SKU Units (#)'}
            combine_st_data.rename(columns=change_columns, inplace=True)
            combine_st_data.to_excel(save_combined_st_path, sheet_name='combined search term', index=False)
            return combine_st_data

        # 获取广告报表
        def get_camp_data(files_save_dirname, station_name, camp_30_sign_word='30天'):
            # 获取广告报表
            station_folder = os.path.join(files_save_dirname, station_name)
            if not os.path.exists(station_folder):
                return
            station_files = os.listdir(station_folder)
            camp_30 = [file for file in station_files if camp_30_sign_word in file]
            if not camp_30:
                print(f'{station_folder}没有30天广告报表.')
                return
            if len(camp_30) >= 1:
                if len(camp_30) > 1:
                    print(f'{station_folder}含有多个广告报表,取最新的一个.')
                camp_30_basename = camp_30[0]
            camp_30_path = os.path.join(station_folder, camp_30_basename)
            camp_30_data = read_files(camp_30_path, sheet_name='Sponsored Products Campaigns')
            if camp_30_data.empty:
                print(f'{camp_30_path}为空表.')
                return
            return camp_30_data

        # 1.得到全部的报表
        if set(['ST', 'BR', 'AO', 'AC', 'AL', 'CP']) != all_file_key:
            lost_file = set(['ST', 'BR', 'AO', 'AC', 'AL', 'CP']) - set(all_file_key)
            print(f'{station_name}缺失 {lost_file}报表.')
        else:
            for key in all_file_dict.keys():
                for i in range(len(all_file_dict[key])):
                    if key == 'AC':
                        ac_data = download_from_api(all_file_dict[key][i], files_save_dirname, station_name, key)
                    elif key == 'AO':
                        ao_data = download_from_api(all_file_dict[key][i], files_save_dirname, station_name, key)
                    else:
                        download_from_api(all_file_dict[key][i], files_save_dirname, station_name, key)
        unified_reports_name(station_save_folder, station_name)
        try:
            combined_st_data = combine_st(files_save_dirname, station_name)
        except Exception as e:
            print(station_name)
            print(e)

        """
        附加功能: 生成处理精否/生成新品自动新增/生成ST新品新增
        """
        # 得到广告报表数据
        camp_data = get_camp_data(files_save_dirname, station_name)

        # result1. 生成处理精否
        if (not combined_st_data is None):
            print(f'{station_name}: 开始精否!!!')
            negative_exact(station_name, camp_data, combined_st_data, ac_data, station_save_folder)
        # result2. 生成新品自动新增
        if (not ac_data is None) & (not ao_data is None) & (not camp_data is None):
            print(f'{station_name}: 开始新品自动新增!!!')
            process_auto_new(station_name, files_save_dirname, ac_data, ao_data, camp_data)
        # result3. 生成ST新品新增
        if (not combined_st_data is None) & (not camp_data is None) & (not ac_data is None):
            print(f'{station_name}: 开始ST关键词新增!!!')
            process_st_new(station_name, combined_st_data, camp_data, ac_data)


# 报表请求结果日志
def request_file_result(all_stations_name):
    request_save_folder = f"{file_load_drive}:/api_request_all_files"
    now_date = str(datetime.now().date())
    request_log_save_path = request_save_folder
    request_save_folder = os.path.join(request_save_folder, now_date)
    if not os.path.exists(request_save_folder):
        return
    requests_stations_name = os.listdir(request_save_folder)
    requests_stations_name = [station for station in requests_stations_name if station in all_stations_name]
    all_file_result = []
    for station_name in requests_stations_name:
        station_folder = os.path.join(request_save_folder, station_name)
        station_request_result = request_log(station_folder)
        all_file_result.append(station_request_result)
    # 没有报表的的站点
    stations_not_post_name = set(all_stations_name) - set(requests_stations_name)
    if stations_not_post_name:
        [all_file_result.append(
            [station_name, '没有', 10, '不存在', '不存在', '不存在', '不存在', '不存在', '不存在', '不存在', '不存在', '不存在',
             '不存在']) for station_name in stations_not_post_name]

    columns_name = ['站点名', '完整情况', '报表缺失数量', 'camp_30', 'order', 'br', 'st', 'al', 'ac', 'camp_1', 'camp_7', 'camp_14',
                    'camp_60']
    now_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    request_result = pd.DataFrame(all_file_result, columns=columns_name)
    summary_columns = ['时间', '全部站点数', '完整报表站点数', '不完整报表站点数', 'camp_30', 'order', 'br', 'st', 'al', 'ac',
                       'camp_1',
                       'camp_7', 'camp_14', 'camp_60']
    summary_data = [now_date, len(all_stations_name),
                    len(request_result[(request_result['camp_30'] == '存在') & (request_result['br'] == '存在')
                                       & (request_result['st'] == '存在') & (request_result['camp_1'] == '存在') & (
                                               request_result['camp_7'] == '存在')
                                       & (request_result['camp_14'] == '存在') & (request_result['camp_60'] == '存在')]),
                    len(request_result[(request_result['camp_30'] == '不存在') | (request_result['br'] == '不存在')
                                       | (request_result['st'] == '不存在') | (request_result['camp_1'] == '不存在') |
                                       (request_result['camp_7'] == '不存在') | (request_result['camp_14'] == '不存在') |
                                       (request_result['camp_60'] == '不存在')]),
                    len(request_result[request_result['camp_30'] == '存在']),
                    len(request_result[request_result['order'] == '存在']),
                    len(request_result[request_result['br'] == '存在']),
                    len(request_result[request_result['st'] == '存在']),
                    len(request_result[request_result['al'] == '存在']),
                    len(request_result[request_result['ac'] == '存在']),
                    len(request_result[request_result['camp_1'] == '存在']),
                    len(request_result[request_result['camp_7'] == '存在']),
                    len(request_result[request_result['camp_14'] == '存在']),
                    len(request_result[request_result['camp_60'] == '存在'])]
    summary_info = pd.DataFrame([summary_data], columns=summary_columns)
    writer = pd.ExcelWriter(os.path.join(request_log_save_path, f'request_log_{now_date}.xlsx'))
    summary_info.to_excel(writer, startrow=2, index=False, sheet_name='站点报表请求情况')
    request_result.to_excel(writer, startrow=6, index=False, sheet_name='站点报表请求情况')
    writer.save()


''' 多线程
def thread_read_file():
    save_folder_path = f"{file_load_drive}:/api_request_all_files"
    while 1:
        all_task = []
        for one_page in range(1):
            all_task.append(THREAD_POOL.submit(request_save_all_6_files))
        for future in as_completed(all_task):
            future.result()
        if stations_queue.isEmpty():
            break
'''


# 通过mac地址匹配获取请求的站点名
def get_stations():
    # 请求的站点数
    stations_name_n_manger = db_download_station_names()

    # 通过数据库获取mac地址表,返回manager,mac两列
    def db_download_manager_mac(db='ad_db', table='login_user', ip='wuhan.yibai-it.com', port=33061,
                                user_name='marmot', password='') -> pd.DataFrame:
        """
        加载所有用户的mac地址
        :return: 用户的mac地址
        """
        conn = pymysql.connect(
            host=ip,
            user=user_name,
            password=password,
            database=db,
            port=port,
            charset='UTF8')
        # 创建游标
        cursor = conn.cursor()
        # 写sql
        sql = """SELECT real_name,pc_mac FROM {} """.format(table)
        # 执行sql语句
        cursor.execute(sql)
        all_manager_mac = cursor.fetchall()
        all_manager_mac = pd.DataFrame([list(mac) for mac in all_manager_mac],
                                       columns=['manager', 'mac'])
        all_manager_mac.drop_duplicates(inplace=True)
        conn.commit()
        cursor.close()
        conn.close()
        return all_manager_mac

    # 控制每人只获取自己的站点数据
    # self_mac = ':'.join(['{:02x}'.format((uuid.getnode() >> ele) & 0xff) for ele in range(0, 8 * 6, 8)][::-1])
    self_mac = 'B2:C0:90:0C:B8:45'
    all_manager_mac = db_download_manager_mac()
    manager_name = [manager for manager, mac in zip(all_manager_mac['manager'], all_manager_mac['mac']) if
                    self_mac.upper() in mac.upper()]
    if not manager_name:
        print(f"mac不存在: {self_mac}不在mac地址表中")
        root = tkinter.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        tkinter.messagebox.showinfo('五表请求', f'五表请求错误: {self_mac} 不在mac库中,请联系管理员添加mac.')
        raise ('quit')
    manager_name = manager_name[0]
    stations_name = stations_name_n_manger['station_name'][stations_name_n_manger['manger'] == manager_name]
    stations_name = pd.Series(['doqo_ca', 'doqo_mx'])
    # 单独请求某个站点的数据
    # stations_name = pd.Series(['nineone_jp'])
    if stations_name.empty:
        print(f"姓名不存在: {manager_name}不在only_station_info数据中")
        root = tkinter.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        tkinter.messagebox.showinfo('无表情求', f'五表请求错误: {manager_name} 错误.\n请联系管理员核查mac地址中的姓名和only_station_info中的姓名是否一致.')
        raise ('quit')

    # 将站点写入队列
    stations_queue = Queue()
    if len(stations_name) == 0:
        return
    for station in stations_name:
        stations_queue.enqueue(station)

    return stations_queue


# 设置文件存储路径
def find_drives():
    drives = win32api.GetLogicalDriveStrings()
    drives = drives.upper()
    if 'D' in drives:
        drives = 'D'
    elif 'E' in drives:
        drives = 'E'
    elif 'F' in drives:
        drives = 'F'
    else:
        drives = 'C'
    return drives


if __name__ == '__main__':
    stations_queue = get_stations()
    stations_num = stations_queue.size()
    all_stations = stations_queue.items.copy()
    print(f"此次请求{stations_num}个站点.")
    file_load_drive = find_drives()
    '''主程序'''
    while 1:
        try:
            request_save_all_6_files()
        except Exception as e:
            print(e)
        if stations_queue.isEmpty():
            break
    print(f"{stations_num}个站点全部完成.")
    request_file_result(all_stations)
    root = tkinter.Tk()
    root.withdraw()
    root.wm_attributes('-topmost', 1)
    tkinter.messagebox.showinfo('五表请求结果',
                                f'五表请求完成. 一共请求{stations_num}个站点.\n文件存储在{file_load_drive}:/api_request_all_files下.\n'
                                f'报表日志: 请查看文件夹下request_log\n若有站点数据未成功请求,请联系管理员.')
