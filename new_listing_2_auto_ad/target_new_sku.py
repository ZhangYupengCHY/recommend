# -*- coding: utf-8 -*-
"""
Created on  11月13日 14:10 2019

@author: Zhang yp
"""

import pandas as pd
from datetime import datetime
import datetime as dt
import pymysql
import numpy as np
import re

"""
目的:
    找到站点条件目的下的新增的(seller)sku。
逻辑: 
    最新active_listing中筛选的sku - 最新all_order中的sku - 最新bulk中筛选的sku = another_sku   
步骤: 
    1. 最新active_listing中筛选‘日期’,'客单价','发货方式'筛选出active_listing的sku
    2. 得到all_orders表中的sku列表
    3. 得到广告报表中的sku，并按照是否包含暂停分为两类
    4. 用最新active_listing中筛选的sku - 最新all_order中的sku - 最新bulk中筛选的sku = another_sku   
    5. another_sku为0,进分类模型
        another_sku不为0
        sku有bid  bid
   
"""


def read_txt(txt_path, sep='\t') -> pd.DataFrame:
    """
    将bytes格式的txt读成dataframe
    :param txt_path:txt文件路径
    :param sep:txt文件的分隔符
    :return: txt的dataframe格式
    """
    with open(txt_path, 'rb') as f:
        txt_file = f.readlines()
    txt_file = [one_line.decode('utf-8', errors='ignore').split(sep) for one_line in txt_file]
    df_txt = pd.DataFrame(txt_file[1:], columns=txt_file[0])
    return df_txt


def read_active_listing(file_path) -> pd.DataFrame:
    """
    读取active_listing数据
    :param: file_path:active_listing文件路径
    :return: active_listing数据
    """
    active_listing = read_txt(file_path)
    useful_columns = ['seller-sku', 'price', 'asin1', 'open-date', 'fulfillment-channel']
    return active_listing[useful_columns]


def db_select_sellersku_bid(sellersku, site, db='sellersku_performance', ip='192.168.129.240',
                            user_name='marmot',
                            password='', port=3306) -> list:
    """
    分国家取出每个sellersku的bid,若一个sellersku存在几个bid，则取最大impression的bid
    :param sellersku: sellersku的列表
    :param site:站点国家
    :param db:数据库名
    :param ip:服务器ip
    :param user_name:账号
    :param password:密码
    :param port:端口
    :return:
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
    table_name = site + '_sellersku_perf'
    sql = """SELECT max_bid,impressions FROM {} where sku = {} """.format(table_name, "'%s'" % sellersku)
    # 执行sql语句
    cursor.execute(sql)
    all_result = np.array(cursor.fetchall())
    if len(all_result) == 1:
        # 若sellersku只有一个bid
        sellersku_bid = all_result[0]
    else:
        # 若sellersku有很多bid,则取曝光最大值的bid
        sellersku_bid = \
            [bid for bid, impression in zip(all_result[:, 0], all_result[:, 1]) if impression == max(all_result[:, 1])][
                0]
    return sellersku_bid


def new_sellersku_bid(another_sellersku: dict, site):
    """
    通过新的sellersku有没有同类sellersku，来给new_sellersku出价
    若sellersku没有同类，则进入分类模型
    若sellersku有同类，则将同类的sellersku的bid作为new_sellersku出价的依据
    :param another_sku:new_sellersku同类sellersku
    :param site:new_sellersku的国家
    :return:new_sellersku的出价
    """
    for new_sellersku, another_skus in another_sellersku.items():
        if another_skus:
            # 进入通过多个sellersku的bid来判断new_sellersku的bid
            another_sellersku_bid = [db_select_sellersku_bid(sku) for sku in another_skus]
            pass
        else:
            # 进入分类模型
            pass


def choose_part_active_listing(active_listing_info, start_date=str(datetime.now().date() - dt.timedelta(30)),
                               end_date=str(datetime.now().date()), min_price=7, ship_type='DEFAULT') -> pd.DataFrame:
    """
    选择符合要求的active_listing_data
    :param active_listing_info: 全部的active_listing数据
    :param start_date: 开始日期
    :param end_date: 结束日期
    :param min_price: 最小价格
    :param ship_type: 发货方式
    :return: 满足要求的部分active_listing数据
    """
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    except:
        print('line 65: 请将时间的输入格式改为:xxxx-mm-dd 例如2019-11-13 ')
    if isinstance(min_price, str):
        min_price = float(min_price)
    if ship_type.upper() == 'DEFAULT':
        choose_data = active_listing_info[
            (active_listing_info['open-date'] >= start_date) & (active_listing_info['open-date'] <= end_date) & (
                    active_listing_info['price'] >= min_price) & (
                    active_listing_info['fulfillment-channel'] == 'DEFAULT')]
    else:
        choose_data = active_listing_info[
            (active_listing_info['open-date'] >= start_date) & (active_listing_info['open-date'] <= end_date) & (
                    active_listing_info['price'] >= min_price) & (
                    active_listing_info['fulfillment-channel'] != 'DEFAULT')]
    return choose_data


def init_active_listing(ori_active_listing):
    """
    将原始的active_listing就行初始化处理
    需要处理的列为:日期列,客单价列
    :param ori_active_listing: 原始的active_listing数据
    :return: 处理后的active_listing数据
    """
    init_data = ori_active_listing.copy()
    init_data['price'] = init_data['price'].apply(lambda x: float(x) if not isinstance(x, float) else x)
    init_data['open-date'] = init_data['open-date'].apply(lambda x: datetime.strptime(x[:10], '%Y-%m-%d'))
    return init_data


def get_active_listing_sku_info(path, start_date=str(datetime.now().date() - dt.timedelta(30)),
                                end_date=str(datetime.now().date()), min_price=7, ship_type='DEFAULT'):
    """
    读取active_listing，并将active_listing进行初始化处理然后得到筛选后的数据
    :param path: active_listing路径
    :param start_date: active_listing开始日期
    :param end_date: active_listing开始日期
    :param min_price: 最小的客单价
    :param ship_type: 发货方式 默认为FBM   另外为FBA
    :return: 筛选后的active_listing值
    """
    ori_active_listing = read_active_listing(path)
    inited_active_listing = init_active_listing(ori_active_listing)
    needed_active_listing = choose_part_active_listing(inited_active_listing, start_date=start_date, end_date=end_date,
                                                       min_price=min_price, ship_type=ship_type)
    needed_active_listing.reset_index(drop=True, inplace=True)
    return needed_active_listing


def get_orders_sku(path, sku_columns_name='sku') -> list:
    """
    读取all_orders,获得sku列表
    :param path:all_orders 文件路径
    :param sku_columns_name:SKU所在的列名 “'sku'”
    :return: all_orders的sku列表
    """
    all_order_data = read_txt(path)
    all_order_sku_list = list(all_order_data[sku_columns_name])
    return all_order_sku_list


def get_ad_sku(path, include_pause=True) -> list:
    """
    得到是否包含暂停的广告报表的sku列表
    :param path: 广告报表的路径
    :param include_pause: 是否包含暂停的sku
    :return:
    """
    writer = pd.ExcelFile(path)
    if 'Sponsored Products Campaigns' not in writer.sheet_names:
        print('error type1: {}工作簿中不存在Sponsored Products Campaigns工作表.'.format(path.split('/')[-1]))
        return []
    try:
        ad_sku_data = writer.parse(sheet_name='Sponsored Products Campaigns', usecols=['SKU', 'Status'])
    except Exception as e:
        print("error type2: ['SKU','Status']列名不存在 {}.".format(path.split('/')[-1]))
        print(e)
        return []
    ad_sku_data.dropna(inplace=True)
    ad_sku_data.drop_duplicates(inplace=True)
    if not include_pause:
        ad_sku_data = ad_sku[ad_sku_data['Status'] == 'enabled']
    ad_sku_list = list(set(ad_sku_data['SKU']))
    return ad_sku_list


def db_download_erpsku_sellersku(ip='39.108.85.90', port=3306, user='libo',
                                 password='Lbdataro2019***',
                                 database='yibai_product',
                                 table='yibai_amazon_sku_map', request_rows_onetime=1000000):
    """
    连接数据库，得到erpsku与sellersku信息
    :param ip: 主机ip
    :param port: 主机端口
    :param user: 用户名
    :param password: 密码
    :param database: 数据库名
    :param table: 数据库表
    :param have_order_one_month: 是否需有出单 （默认的不需要筛除掉不出单）
    :return: erpsku与sellersku对应表以及asin
    """
    conn = pymysql.connect(
        host=ip,
        user=user,
        password=password,
        database=database,
        port=port,
        charset='UTF8')

    # 创建游标
    cursor = conn.cursor()
    # 写sql
    sql = """SELECT seller_sku,sku,account_name FROM {} where account_name LIKE 'serounder%' """.format(table)
    # 执行sql语句
    cursor.execute(sql)
    all_result = cursor.fetchall()
    erpsku_sellersku = pd.DataFrame([list(j) for j in all_result], columns=['seller_sku', 'sku', 'account_name'])
    # 从标识中提取seller_sku以及sellersku的国家
    # country_dict={'英国':'uk','美国':'us','':}
    erpsku_sellersku['site'] = erpsku_sellersku['account_name'].apply(lambda x: re.sub('[a-zA-Z站]', '', x))
    del erpsku_sellersku['account_name']
    erpsku_sellersku.dropna(inplace=True)
    erpsku_sellersku.drop_duplicates(inplace=True)
    conn.commit()
    cursor.close()
    conn.close()
    return erpsku_sellersku


def find_another_sellersku(new_sellersku, erpsku_sellersku) -> dict:
    """
    找到同erpsku下新seller_sku的其他seller_sku
    :param new_sku: 新sku的
    :param erpsku_sellersku:erpsku_sellersku对应表
    :return:返回同erpsku下其他sellersku
    """
    new_sellersku_erpsku = pd.merge(new_sellersku, erpsku_sellersku, left_on='seller-sku', right_on='seller_sku',
                                    how='left')
    all_sellersku = pd.merge(new_sellersku_erpsku, erpsku_sellersku, on='sku', how='inner')
    new_sellersku_grouped = all_sellersku[['seller-sku', 'seller_sku_y', 'site']].groupby(['seller-sku'])
    another_sellersku = {key: {'sku': list(value['seller_sku_y'].values), 'bid': list(value['site'].values)} for
                         key, value in new_sellersku_grouped}
    for key, value in another_sellersku.items():
        index_sku = value['sku'].index(key)
        del value['sku'][index_sku]
        del value['site'][index_sku]
    return another_sellersku


if __name__ == "__main__":
    # active_listing的文件路径
    active_listing_path = 'ad_helper/recommend/new_listing_2_auto_ad/static_files/Serounder_US_active_listing_2019-11-18.txt'
    # 读取达到要求的active_listing文件
    # 默认的是最近30，客单价最低为7 发货方式为FBM
    active_listing_data = get_active_listing_sku_info(active_listing_path,
                                                      start_date=str(datetime.now().date() - dt.timedelta(30)),
                                                      end_date=str(datetime.now().date()), min_price=7,
                                                      ship_type='DEFAULT')
    # allorders文件路径
    all_orders_path = 'ad_helper/recommend/new_listing_2_auto_ad/static_files/Serounder_us_allorders_2019-11-17.txt'
    # 读取allorders文件下的所有sku
    all_orders_sku = get_orders_sku(all_orders_path)
    # 广告报表的文件路径
    ad_path = 'ad_helper/recommend/new_listing_2_auto_ad/static_files/874_Serounder美国_AmazonSponsoredProductsBulk_2019-11-16.xlsx'
    # 得到广告报表的是否包含暂停的sku
    ad_sku = get_ad_sku(ad_path, include_pause=True)
    # 从筛选后的active_listing中排除allorders和广告报表中符合条件的sku，最后得到满足条件的new_sku
    new_sku_info = active_listing_data[~active_listing_data['seller-sku'].isin(all_orders_sku + ad_sku)]
    new_sku_info.reset_index(drop=True, inplace=True)
    # 连接数据库，得到erpsku与sellersku信息
    erpsku_sellersku = db_download_erpsku_sellersku(ip='39.108.85.90', port=3306, user='libo',
                                                    password='Lbdataro2019***',
                                                    database='yibai_product',
                                                    table='yibai_amazon_sku_map')
    # 找到new_sku下其他sellersku
    new_sellersku_another_sellersku = find_another_sellersku(new_sku_info, erpsku_sellersku)
