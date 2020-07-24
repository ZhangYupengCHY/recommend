#!/usr/bin/env python
# coding=utf-8
# author:marmot

import glob
import pandas as pd


def get_min_cr(br_df, camp_df):
    # 处理Sessions列
    br_df['Sessions'] = br_df['Sessions'].apply(lambda x: int(str(x).replace(",", '')))
    # 用订单和除以曝光的和得到BR的转换率
    cr_br = sum(br_df['Units Ordered']) / sum(br_df['Sessions'])

    # 用Campaign下的Orders除以Clicks得到订单的转换率
    camp_df = camp_df[camp_df['Record Type'] == 'Campaign']
    cr_camp = sum(camp_df['Orders']) / sum(camp_df['Clicks'])

    # 比较店铺的转化率和广告转换率
    cr_min = min(cr_br, cr_camp)

    return cr_min


def imp_files(dir_name):
    # 导入文件夹目录
    # 得到文件夹下的所有文件名称
    # 得到店铺文件名和订单文件名
    br_dir = glob.glob(dir_name + '/' + 'BusinessReport*')[0]
    camp_dir = glob.glob(dir_name + '/' + 'bulksheet*')[0]
    # 导入文件数据
    br_data = pd.read_csv(br_dir)
    # 默认sheet_name = 'Sponsored Products Campaigns'
    camp_data = pd.read_excel(camp_dir)

    return br_data, camp_data


def get_bid(bs_data):
    # 获得只含有Record Type为Capmaign，放置重复计算
    camp = bs_data[bs_data['Record Type'] == 'Campaign']
    # 计算cpc = 花费/点击
    cpc = sum(camp['Spend']) / sum(camp['Clicks'])
    # 计算最小与最大出价
    min_bid = 0.7 * cpc
    max_bid = 1.3 * cpc
    return min_bid, max_bid


def get_min_cr_and_bid(dir_name):
    # 导入BusinessReport和bulksheet文件
    br_data, bs_data = imp_files(dir_name)
    # 取CR广告和CR店铺的小值
    cr_min = get_min_cr(br_data, bs_data)
    # 计算最小的出价和最高的出价
    min_bid, max_bid = get_bid(bs_data)
    return cr_min, min_bid, max_bid


if __name__ == '__main__':
    dir_name_test = r"D:\待处理\AMOQ\AMOQ_UK"
    a, b, c = get_min_cr_and_bid(dir_name_test)
