# -*- coding: utf-8 -*-
"""
"Proj: ad_helper",
Created on:   2020/5/22 17:40
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
from datetime import datetime
import public_function

sale_exchange_rate = {'CA': 0.7519, 'DE': 1.0981, 'FR': 1.0981, 'IT': 1.0981, 'SP': 1.0981, 'JP': 0.009302,
                      'UK': 1.2445, 'MX': 0.05147, 'IN': 0.01412, 'US': 1, 'ES': 1.0981, 'AU': 0.6766, 'AE': 0.2723}

if __name__ == "__main__":
    station_info = public_function.db_download_table('only_station_info')
    station_mode = public_function.db_download_table('station_mode')
    team_member = public_function.db_download_table('access_assign')
    # 1.连接三个表
    all_info = pd.merge(station_info, station_mode, left_on='station', right_on='account', how='left')
    all_info = pd.merge(all_info, team_member, left_on='ad_manger', right_on='real_name', how='left')

    # 调整acos列
    all_info['acos'] = [0 if (pd.isnull(acos)) or (acos in [' ', '']) else float(acos.strip('% ')) / 100 for acos in
                        all_info['acos']]
    # 2.计算的字段
    # spend 花费占比 距离上次操作时间
    all_info['spend'] = all_info['ad_sales'] * all_info['acos']
    all_info['花费占比'] = [spend / sale if sale > 0 else 0 for spend, sale in
                        zip(all_info['spend'], all_info['shop_sales'])]
    now_date = datetime.now()
    all_info['操作时间间隔'] = [
        (int(str(now_date.year)[-2:]) - int(day[:2])) * 365 + (now_date.month - int(day[3:5])) * 30 + (
                now_date.day - int(day[6:8])) for day in all_info['update_time']]
    first_class_admin = all_info[['real_name', 'team_member']][all_info['position'] == '组长']
    # all_info['组长'] = [real_name for manager in all_info['ad_manger'] for
    #                   team_member, real_name in zip(first_class_admin['team_member'], first_class_admin['real_name']) if ((manager in team_member) or (manager == real_name))]
    # print(all_info['组长'])
    # 找组长
    class_admin = []
    for ad_manger in all_info['ad_manger']:
        ad_manger_admin = None
        for team_member, real_name in zip(first_class_admin['team_member'], first_class_admin['real_name']):
            if (ad_manger in team_member) or (ad_manger==real_name):
                ad_manger_admin = real_name
                break
        class_admin.append(ad_manger_admin)
    all_info['组长'] = class_admin
    # 计算本币
    all_info['site'] = all_info['station'].apply(lambda x: x[-2:].upper())
    for column in ['spend', 'ad_sales', 'shop_sales', 'cpc']:
        all_info[column] = [round(column * sale_exchange_rate[site], 2) for column, site in
                            zip(all_info[column], all_info['site'])]

    # 3.筛选
    # 精品花费占比>8%,其他花费占比>6%
    select_info_1 = all_info[((all_info['mode'] == 'precious') & (all_info['花费占比'] > 0.08))]
    select_info_2 = all_info[((all_info['mode'] != 'precious') & (all_info['花费占比'] > 0.06))]
    select_info = select_info_1.append(select_info_2)

    # 4.输出的列
    select_info = select_info[
        ['station', 'mode', 'acos', 'spend', 'cpc', 'ad_sales', 'shop_sales', '花费占比', 'percentage', '操作时间间隔', 'note',
         'ad_manger','组长']]
    select_info.rename(
        columns={'station': '站点', 'mode': '模式', 'acos': 'ACoS', 'spend': '花费', 'ad_sales': '广销额', 'shop_sales': '店销额',
                 'percentage': '广销比', '操作时间间隔': '距上次操作', 'note': '备注', 'ad_manger': '广告接手人'}, inplace=True)
    select_info.to_excel('D:/temp_export/精品花费占比大于0.08 其他花费占比大于0.06.xlsx',index=False)
