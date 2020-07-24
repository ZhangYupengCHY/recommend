# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/6/3 14:15
@Author: RAMSEY

"""
import os

import my_toolkit.process_files as process_files
import my_toolkit.public_function as public_function
from datetime import datetime,timedelta
import time

"""
概述:
    将不同数据来源的五表pickle化后,方便读取
实现逻辑:
        将销售与广告专员下载的压缩包解压后,将文件解压成pickle,
        文件名为:账号_站点_表类型.pkl,
        将账号_站点_表类型+事件key存储在redis中
"""


def zipped_folders_2_pickle(zipped_files: 'path',unzipped_file_save_folder=None,folder_save_pkl=False,delete_file=True) -> None:
    """
    将存储压缩文件夹的压缩文件中的文件序列化存储为pickle,同时将路径存在在redis中
    Parameters:
        zipped_files:path object
                     销售压缩文件夹的压缩文件
        folder_save_pkl:path object,or False,default False
                        存储pickle文件的文件夹,默认解压到当前文件夹
        delete_file:bool,default True
                    是否删除压缩后的文件,默认为删除文件
    Returns: None
    """
    if not os.path.exists(zipped_files):
        raise FileNotFoundError('{} cant found.'.format(zipped_files))
    # 若没有指定解压文件存储的文件夹,则将压缩文件压缩到当前文件夹
    if unzipped_file_save_folder is None:
        unzipped_file_save_folder = os.path.dirname(zipped_files)
    if not os.path.isdir(unzipped_file_save_folder):
        raise ValueError('{} is not a folder.'.format(unzipped_file_save_folder))
    if not os.path.exists(unzipped_file_save_folder):
        os.mkdir(unzipped_file_save_folder)
    # 若没有指定文件夹,则解压pkl文件指定为文件夹
    if not folder_save_pkl:
        folder_save_pkl = unzipped_file_save_folder
    if not os.path.isdir(folder_save_pkl):
        raise ValueError('{} is not a folder.'.format(folder_save_pkl))
    if not os.path.exists(folder_save_pkl):
        os.mkdir(folder_save_pkl)
    # 1. 解压文件
    process_files.unzip_folder(zipped_files, save_folder=unzipped_file_save_folder)
    # 2. 将文件pkl化
    # 2.1 获得全部文件完整路径
    all_files = []
    station_name = os.path.splitext(os.path.basename(zipped_files))[0]
    station_name = station_name.upper()
    station_folder = os.path.join(unzipped_file_save_folder,station_name)
    for root, _, files in os.walk(station_folder):
        for file in files:
            file_path = os.path.join(root, file)
            all_files.append(file_path)

    # 2.2 pickle化(这里文件的sheet名应该规范化,防止其他语言)
    # 保存pickle到当前文件夹下面
    # 规范输出后的pkl文件名(账号_站点_数据类型)
    def standardize_file_pickle_name(file_path):
        """
        规范文件输出后pickle文件命名(账号_站点_数据类型_时间)
        :param file_path:
        :return:
        """
        if not os.path.isfile(file_path):
            raise FileExistsError('{} not a file.')
        if not os.path.exists(file_path):
            raise FileExistsError('{} not exists.')
        station_name = os.path.basename(os.path.dirname(file_path))
        station_name = station_name.upper()
        account = station_name[:-3]
        site = station_name[-2:]
        # 关键词判断无法判断all order表
        file_type = [type for type, keyword in public_function.FILE_RECOGNIZE.items() if keyword in file_path.lower()]
        if len(file_type) == 1:
            file_type = file_type[0]
        else:
            if os.path.splitext(os.path.basename(file_path))[0].isdigit():
                file_type = 'ao'
            else:
                file_type = 'None'
        return account + '_' + site + '_' + file_type + '.pkl'

    # 将保存为pickle同时将文件路径保存在redis中
    redis_store = public_function.Redis_Store()
    keys = redis_store.keys()
    # 压缩文件的时间
    last_time_timestamp = os.path.getctime(zipped_files)
    last_time = datetime.fromtimestamp(last_time_timestamp).strftime('%Y%m%d%H%M%S')
    sign_key = 'FIVE_FILES_KEYS_SAVE'
    for file_path in all_files:
        file_pickle_path = os.path.join(folder_save_pkl, standardize_file_pickle_name(file_path))
        process_files.write_file_2_pickle(file_path, pkl_path=file_pickle_path)
        # 保存的键为站号_站点_文件类型_时间
        # redis保存的键为five_save_keys+站点+日期 five_save_keys为项目标志
        file_redis_key = sign_key + ':' + standardize_file_pickle_name(file_path).replace('.pkl','') + '_' + last_time
        file_redis_key = file_redis_key.upper()
        redis_store.set(file_redis_key, file_pickle_path)
        # 删除该站点之前存储的键
        [redis_store.delete(key) for key in keys if
         (sign_key in key) and (station_name in key) and (last_time not in key)]
        if delete_file:
            os.remove(file_path)



def refresh_folder(zipped_folder,unzipped_file_save_folder=None,folder_save_pkl=False):
    """
    判断文件夹中更新压缩文件,
    然后对压缩文件进行解压pickle化处理
    Parameters:
        zipped_folder:path object
                      销售压缩文件存放的文件夹
    Returns:None
    """
    if not os.path.isdir(zipped_folder):
        raise ValueError(f'{zipped_folder} is not a path.Please input saler zipped folder path')
    if not os.path.exists(zipped_folder):
        return
    processed_stations = set()
    while 1:
        now_date = datetime.now().date()
        # now_date -= timedelta(days=5)
        stations_dir = [os.path.join(zipped_folder,zipped_file) for zipped_file in os.listdir(zipped_folder) if '.zip' in zipped_file.lower()]

        zipped_files_info = set(
            [station_dir + '_' + time.ctime(os.path.getmtime(station_dir)) for station_dir in
             stations_dir if
             datetime.strptime(time.ctime(os.path.getmtime(station_dir)), '%a %b %d %H:%M:%S %Y').date() <= now_date])

        # 需要处理的今天站点信息
        needed_process_station = zipped_files_info - processed_stations
        # 需要处理站点的压缩包路径
        needed_process_station_zip_namelist = [station[:-25] for station in
                                               needed_process_station]
        print(f'处理站点总数为：{len(needed_process_station_zip_namelist)}')

        if len(needed_process_station_zip_namelist) > 0:
            for one_station_zipped in needed_process_station_zip_namelist:
                try:
                    zipped_folders_2_pickle(one_station_zipped,unzipped_file_save_folder=unzipped_file_save_folder,folder_save_pkl=folder_save_pkl)
                except Exception as e:
                    station_name = os.path.splitext(os.path.basename(one_station_zipped))[0]
                    print(f'{station_name} not write into pickle.')
        processed_stations.update(needed_process_station)
        print("暂时没有站点更新，休息1分钟...")
        time.sleep(60)


if __name__ == "__main__":
    zipped_folder = r"C:\Users\Administrator\Desktop\sales_upload_zipped"
    pkl_save_folder = r"C:\Users\Administrator\Desktop\temp"
    refresh_folder(zipped_folder,folder_save_pkl=pkl_save_folder)
