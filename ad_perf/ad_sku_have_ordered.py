#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/22 0022 14:57
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : ad_sku_have_ordered.py


from datetime import datetime, timedelta

import pandas as pd

from my_toolkit import public_function
from my_toolkit import process_files
from my_toolkit import commonly_params
from my_toolkit import init_station_report

"""
近7天近14天近30天做上广告出单了的sku及竞价、广告数据生成一张报表
    包含字段:
        账号名,广告日期,sku,erp sku,asin,（后期加上类目）,广告出单量,广告竞价,广告花费,广告销售额,acos
    字段来源:
        账号名:文件路径中提取
        广告日期:广告报表中Ad Group列提取
        sku:广告报表中SKU列
        erp sku:通过sku与erp sku对应表(通过读取数据库存储在redis中,直接在redis中取)
        asin:active listing表中asin1列(通过sku匹配)
        类目:通过接口获取
        广告出单量:广告报表中Orders列
        广告竞价:广告报表中Max Bid列
        广告花费:广告报表中Spend列
        广告销售额:广告报表中Sales列
        acos:广告报表中ACoS列

"""


def find_new_station(date_range=1) -> list:
    """
    找到需要更新站点列表
    Args:
        date_range (int) default 1:
        全部站点中需要
    Returns:list
        需要计算的站点列表

    """
    # 初始化redis
    try:
        redis_conn = public_function.Redis_Store(db=2)
    except:
        redis_conn.close()
        raise ConnectionError('Can not connect redis.')
    # 获取站点中五表时间
    five_files_redis_sign = commonly_params.five_files_redis_sign
    all_redis_keys = redis_conn.keys()
    redis_conn.close()
    five_files_redis_keys = [key for key in all_redis_keys if five_files_redis_sign in key]
    # 每个redis键的最后14位为报表上传时间,站点信息在'FIVE_FILES_KEYS_SAVE:02_AU_AC_20200718105127'
    # redis键由:20位标识符('FIVE_FILES_KEYS_SAVE')+站点+2位报表名称+14位时间标识符组成
    # 从今日向前取date_range天数的站点
    now_date = datetime.today().date()
    start_date = now_date - timedelta(days=date_range)
    return [key[21:-18] for key in five_files_redis_keys if
            (datetime.strptime(key[-14:], '%Y%m%d%H%M%S').date() >= start_date) & (
                    datetime.strptime(key[-14:],
                                      '%Y%m%d%H%M%S').date() < now_date)]


def load_station_campaign_report(station_name):
    """
    加载站点广告报表
    Args:

        station_name: str
            站点名
    Returns:pd.DataFrame
        返回的站点报表数据

    """
    redis_conn = public_function.Redis_Store(db=2)
    five_files_redis_sign = commonly_params.five_files_redis_sign
    all_redis_keys = redis_conn.keys()
    station_report_key = [key for key in all_redis_keys if
                          (five_files_redis_sign in key) & (station_name.upper() in key) & ('CP' in key)]
    if station_report_key != 1:
        raise ValueError(f'{station_name}_CP have multiple redis key or None.please check redis database')
    station_report_key = station_report_key[0]
    station_report_pkl_path = redis_conn.get(station_report_key)
    redis_conn.close()
    return process_files.read_pickle_2_df(station_report_pkl_path)


def load_sku_erpsku():
    """
    加载sku与erpsku以及asin:
        需要考虑到解码:
            由于erpsku的信息是编码存储到redis中,而erpsku键是解码存储到redis中,
            于是先解码取出erpsku,然后在编码取出erpsku信息
    Returns:pd.DataFrame
    """

    # 1.从redis中获得erpsku信息,若redis中没有，则将信息从数据库中加载到redis中
    # erpsku信息存储在redis中的键是以 erpsku_info_日期_小时
    conn_redis = public_function.Redis_Store(decode_responses=True, db=0)
    redis_db0_keys = conn_redis.keys()
    erpsku_redis_key_sign = commonly_params.erpsku_redis_sign
    erpsku_exist_key = [key for key in redis_db0_keys if erpsku_redis_key_sign in key][0]
    conn_redis.close()
    conn_redis = public_function.Redis_Store(decode_responses=False, db=0)
    erpsku_info = conn_redis.redis_download_df(erpsku_exist_key)
    conn_redis.close()
    return erpsku_info


def ad_sku_have_ordered(date_range=1000):
    """
    获取广告出单sku相关字段主函数.
        默认为每天对前一天的新的站点数据进行处理
        从广告报表以及sku、erp sku和asin对应关系表中取出
            :账号名,广告日期,sku,erp sku,asin,（后期加上类目）,广告出单量,广告竞价,广告花费,广告销售额,acos
        存储到广告组服务器表中.
    步骤:
        1.找到需要处理的站点列表
        2.加载站点全部数据:广告报表和sku/erp sku/asin对应关系表
        3.初始化数据来源
        4.将所需字段输出到广告组服务器中

    Args:
        date_range (int) default 1:
        处理时间段内的站点数据
    Return: None

    """
    # step1.找到需要处理的站点列表
    # 计算时间段内站点信息
    all_new_station = find_new_station(date_range=date_range)
    if not all_new_station:
        start_date = datetime.today().date() - timedelta(days=date_range)
        yesterday = datetime.today().date() - - timedelta(days=1)
        if yesterday != start_date:
            print('**********************************')
            print(f'{start_date}-{yesterday}没有新的站点.')
            print('**********************************')
        else:
            print('**********************************')
            print(f'{yesterday}没有新的站点.')
            print('**********************************')
        return
    # step2.加载站点数据
    # 从pkl文件夹中加载广告报表和active listing
    # 从redis的sku erpsku对应表加载erp sku信息
    # 首先加载sku/erp sku/asin三者关系表
    sku_erpsku_asin = load_sku_erpsku()
    # 加载站点的cp数据
    for station in all_new_station:
        station_campaign_data = load_station_campaign_report(station)
        # 初始化数据
        report_type = 'cp'
        init_station_report.init_report(station_campaign_data, report_type)
        # 得到订单量大于0的sku,以及得到sku的日期
        order_column_name = 'Orders'
        ad_group_column_name = 'Ad Group'
        # 得到订单大于0的sku
        have_ordered_sku_data = station_campaign_data[
            (station_campaign_data['Orders'] > 0) & (station_campaign_data['Record Type'] == 'Ad')]
        # 得到sku的订单时间
        # Ad Group列最后6位为时间,倒数第7为下划线
        invalid_upload_time_sku_sign = ''
        have_ordered_sku_data['sku_upload_time'] = [
            ad_group[-6:] if (ad_group[-7] == '_') & (ad_group[-6:].isdigit()) else invalid_upload_time_sku_sign for
            ad_group in have_ordered_sku_data['Ad Group']]
        # 筛出掉没有上传时间的sku
        have_ordered_sku_data = have_ordered_sku_data[
            have_ordered_sku_data['sku_upload_time'] != invalid_upload_time_sku_sign]
        # 将sku上传时间列转换为日期格式
        have_ordered_sku_data['sku_upload_time'] = have_ordered_sku_data['sku_upload_time'].apply(
            lambda x: datetime.strptime(x, '%y%m%d'))
        # 上架天数列
        now_datetime = datetime.today()
        have_ordered_sku_data['upload_days'] = have_ordered_sku_data['sku_upload_time'].apply(
            lambda x: (now_datetime - x).days)

        # 添加账号列
        have_ordered_sku_data['station'] = station
        # 提取需要的列:账号名,广告日期,sku,（后期加上类目）,广告出单量,广告竞价,广告花费,广告销售额,acos
        extract_columns = ['station', 'SKU', 'upload_days', 'Max Bid', 'Orders', 'Spend', 'Sales', 'ACoS']
        # 保留广告报表需要列
        have_ordered_sku_data = have_ordered_sku_data[extract_columns]

        # 从erpskuinfo中添加erpsku和asin列
        have_ordered_sku_data = pd.merge(have_ordered_sku_data, sku_erpsku_asin, how='left', left_on='SKU',
                                         right_on='erpsku')


if __name__ == '__main__':
    info = ad_sku_have_ordered()
