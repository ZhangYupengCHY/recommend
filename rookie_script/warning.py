# -*- coding: utf-8 -*-
"""
Created on Tue Aug 20 15:23:41 2019

@author: Administrator
由于广告报表下载的时效性的问题，一般预警的时间为早上，此时广告报表最新为大前天，例如如果今天是11月7日，那么广告报表最新为11月4日

spend/impression 涨幅为20%时预警
ACoS 绝对值为60%时预警
CPC 为各个国家的cpc最高出价的1.5倍时预警（本币）
cpc_max = {'US':0.5,'CA':0.4,'UK':0.4,'DE':0.35,'FR':0.35,'IT':0.3,'ES':0.3,'JP':25,'MX':2.5,'IN':4.5}
cpc_warning_bid = {'US': 0.75,'CA': 0.6,'UK': 0.6,'DE': 0.525,'FR': 0.525,'IT': 0.45,'ES': 0.45,'JP': 37.5,'MX': 3.75,'IN': 6.75}
"""

from datetime import datetime
import datetime as dt
import numpy as np
import pandas as pd
from ad_helper.recommend.rookie_script.stg_calc_2_sql import load_account_info, conn_mysql
import sys
import re

sys.path.append('D:\\AD-Helper1\\ad_helper\\init_proc')


# 对单个站点的impression.spend,acos,cpc预警
def warning_columns(data, site, impression_threshold_rate=0.2, spend_threshold_rate=0.2, acos_threshold_rate=0.6):
    global today, list_date
    data['date'] = data['date'].map(lambda x: datetime.strptime(x, '%Y-%m-%d').date() if isinstance(x, str) else x)
    list_date = list(data['date'])
    data['预警'] = ''
    # cpc的最高值(本币)
    cpc_warning_bid = {'US': 0.75, 'CA': 0.6, 'UK': 0.6, 'DE': 0.525, 'FR': 0.525, 'IT': 0.45, 'ES': 0.45, 'JP': 37.5,
                       'MX': 3.75, 'IN': 6.75, 'SP': 0.45}
    today = datetime.now().date()
    # 由于广告报表都是下午才有的，如果早上请求广告报表，其实时间应该是前一天  这里将广告报表请求时间划分时间为16点
    # 延迟三天的报表
    today = (today - dt.timedelta(days=3))
    yesterday = (today - dt.timedelta(days=1))
    # 1. 对impression，spend进行预警 增加或减少阈值 预警
    for columns_name, columns_warning_rate in zip(['impression', 'spend'],
                                                  [impression_threshold_rate, spend_threshold_rate]):
        if set([yesterday, today]).issubset(set(list_date)):
            today_value = data[columns_name][data['date'] == today].values[0]
            yesterday_value = data[columns_name][data['date'] == yesterday].values[0]
            if yesterday_value > 0:
                ratio = (today_value - yesterday_value) / yesterday_value
            else:
                continue
            if abs(ratio) > 0.2:
                if ratio > 0:
                    data['预警'][data['date'] == today] += '预警!!!{}上升{:.2f}%.'.format(columns_name, ratio * 100)
                else:
                    data['预警'][data['date'] == today] += '预警!!!{}下降{:.2f}%.'.format(columns_name, ratio * 100)

    # 2.对acos与cpc预警， acos,cpc绝对值高于阈值则预警
    data['acos'] = data['acos'].apply(lambda x: float(x) if not isinstance(x, float) else x)
    data['cpc'] = data['cpc'].apply(lambda x: float(x) if not isinstance(x, float) else x)
    if today in list_date:
        acos_value = data['acos'][data['date'] == today].values[0]
        if acos_value > acos_threshold_rate:
            data['预警'][data['date'] == today] += '预警!!!ACoS为{:.2f}%.'.format(acos_value * 100)
        cpc_value = data['cpc'][data['date'] == today].values[0]
        if cpc_value > cpc_warning_bid[site]:
            data['预警'][data['date'] == today] += '预警!!!CPC为{:.2f}.'.format(cpc_value)

    return data


def sort_warning_info(warning_info: 'pd.DataFrame') -> 'pd.DataFrame':
    """
    对输出的预警数组进行排序。首先按照预警的个数（通过'预警'的个数）,然后按照spend的花费正的正序，负的倒序。
    :param warning_info:预警的数组
    :return self:排序后的预警数组
    """
    # 创建预警项warning_counts
    warning_info['warning_counts'] = warning_info['warning_info'].apply(lambda x: len(re.findall('预警', x)))
    # 首先取出spend的变化值

    # 对spend添加 辅助列 首先添加一列 当spend 为正数时为1，为负数时为0，为0时为-1
    # 找到spend这个词在预警中的位置
    spend_word_index = [spend.find('spend') for spend in warning_info['warning_info']]
    # 找到spend的值
    warning_info['spend_change'] = [float(value[index + 7:value.find('%', index)]) / 100 if index != -1 else 0 for index, value
                             in zip(spend_word_index, warning_info['warning_info'])]
    # 给spend的值的符号进行标记，为了排序使用
    warning_info['spend_sign'] = [1 if value > 0 else 0 if value < 0 else -1 for value in warning_info['spend_change']]
    # 为了排序,将spend的值取绝对值，然后去排序
    warning_info['spend_abs'] = [abs(value) for value in warning_info['spend_change']]
    # 将广告报表排序 按照预警项数倒序，spend_sign倒序,spend_abs倒序
    warning_info.sort_values(by=['warning_counts','spend_sign','spend_abs'],ascending=[False,False,False],inplace=True)
    warning_info.drop(columns=['warning_counts','spend_sign','spend_abs','spend_change'],inplace=True)
    return warning_info


# 对全部的站点预警
def warning(df):
    data_columns = ['account', 'site', 'date', 'acos', 'cpc', 'cr', 'spend', 'sales',
                    'impression', 'clicks', '预警']
    all_info = pd.DataFrame(columns=data_columns)
    for stat_country, data in df.groupby(['account', 'site']):
        site = stat_country[1].upper()
        one_df = warning_columns(data, site)
        all_info = pd.concat([all_info, one_df], axis=0, ignore_index=True, sort=None)
    all_info = all_info[(all_info['预警'] != '')]
    all_info.rename(columns={'预警': 'warning_info'}, inplace=True)
    # 对输出的预警进行排序，1.按照预警的个数;2.按照spend的花费
    all_info = sort_warning_info(all_info)
    return all_info


# 初始化预警的结构
def init_warning_info(warning_info):
    warning_info['impression'] = warning_info['impression'].astype('int64')
    warning_info['spend'] = warning_info['spend'].astype('float64')
    warning_info['acos'] = warning_info['acos'].astype('float64')
    warning_info['cpc'] = warning_info['cpc'].astype('float64')


# 将预警的结果写入到数据库中
def write_to_mysql(df):
    # df = df.astype(object).where((pd.notnull(df)), None)
    df = df.astype(object).replace(np.nan, 'None')
    df.drop_duplicates(inplace=True)
    df = np.array(df)
    # 创建连接
    conn = conn_mysql()
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
    sql = """insert ignore into warning (account, site, date, acos, cpc, cr, spend, sales,
 impression, clicks,warning_info) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(sql, all_list)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    warning_info = load_account_info()
    init_warning_info(warning_info)
    all_warning_info = warning(warning_info)
    all_warning_info.drop_duplicates(inplace=True)
    write_to_mysql(all_warning_info)
