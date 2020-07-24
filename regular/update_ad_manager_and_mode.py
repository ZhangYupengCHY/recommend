# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/6/15 11:09
@Author: RAMSEY

"""

"""
定期更新站点销售负责人和模式
"""

import pandas as pd

import my_toolkit.conn_db as conn_db


def update_manager(station_info, new_manager_info):
    """
    更新站点的负责人信息
    Args:
        station_info: DataFrame
        new_manager_info: DataFrame

    Returns:DataFrame


    """
    new_manager_info['station'] = new_manager_info['station'].str.replace('-', '_')
    station_info['owner'] = [
        new_manager_info['owner'][new_manager_info['station'] == station].values[0] if station in new_manager_info['station'].values else owner for station, owner in
        zip(station_info['station'], station_info['owner'])]
    return station_info


def update_mode(station_info, new_mode_info):
    """
    更新站点模式
    """

    new_mode_info['station'] = new_mode_info['station'].str.replace('-', '_')
    station_info['mode'] = [
        new_mode_info['模式'][new_mode_info['station'] == station].values[0] if station in new_mode_info['station'].values else mode for station, mode in
        zip(station_info['account'], station_info['mode'])]
    mode_dict = {'产品线':'product','精品':'precious','自发货':'fbm','海外仓':'fba','其他':'other'}
    for mode_ch_en,mode_us in mode_dict.items():
        station_info['mode'] = station_info['mode'].str.replace(mode_ch_en,mode_us)
    return station_info


def update_manager_mode():
    only_station_info_sql = "SELECT * FROM only_station_info"
    only_station_info = conn_db.read_table(only_station_info_sql)
    station_mode_sql = "SELECT * FROM station_mode"
    station_mode = conn_db.read_table(station_mode_sql)
    update_dir = r"D:\AD-Helper1\ad_helper\recommend\regular\站点销售负责人及模式更新.xlsx"
    update_data = pd.read_excel(update_dir,sheet_name='Sheet2')
    new_only_station_info = update_manager(only_station_info,update_data)
    new_station_mode = update_mode(station_mode,update_data)
    conn_db.to_sql_replace(new_only_station_info,'only_station_info')
    conn_db.to_sql_replace(new_station_mode, 'station_mode')

if __name__ == '__main__':
    update_manager_mode()