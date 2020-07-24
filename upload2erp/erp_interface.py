# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/5/11 10:22
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

"""
描述:
    之前处理文件后,将处理好的文件上传到Amazon,上传到Amazon有两个途径:
        一、直接上传到Amazon(后台);二、上传到erp,erp再将文件上传到Amazon（后台）。
    现在通过erp接口将处理后的文件上传到erp服务器中,服务器再将文件上传到Amazon
步骤:
    将文件处理成文件流测格式,通过测试接口上传文件:
    url:192.168.2.168:80/services/api/advertise/Creatrreportinterface
"""

import pandas as pd
import rsa
import base64
import requests


def upload_files_2_server(station_name, file_path):
    """
    上传文件到erp(测试的上传到胡堰生的电脑中)
    :param station_name: 站点名
    :param file_path: 文件路径
    :return: None
    """

    def check_df(df):
        """
        检测df数据的有效性:
            1.不为None
            2.是pd.DataFrame数据类型
            3.非空
        :param df: pd.DataFrame
        :return: pd.DataFrame
        """
        if (df is None) or (isinstance(df, pd.DataFrame)) or (df.empty):
            return

    def find_station_id(station_name):
        """
        通过站点名,获得站点的id，方便上传
        :param station_name: 站点名
        :return: 站点id
        """
        station_id = '021'
        return station_id

    # 检测df数据的有效性
    # if check_df(station_data) is None:
    #     return
    # 匹配得到站点的id,并判断id的有效性
    station_id = find_station_id(station_name)
    if station_id is None:
        return
    # 通过接口上传数据
    key_path = "D:/month_files/public.key"
    with open(key_path, 'r') as fp:
        public_key = fp.read()
    # pkcs8格式
    key = public_key
    password = "Kr51wGeDyBM39Q0REVkXn4lW7ZqCxdPLS8NO6iIfubTJcvsjt2YpmAgzHFUoah"
    pubkey = rsa.PublicKey.load_pkcs1_openssl_pem(key)
    password = password.encode('utf8')
    crypt_password = rsa.encrypt(password, pubkey)
    token = base64.b64encode(crypt_password).decode()

    # 普通字段
    upload_param ={
        'token':token,
        'account_id':23,
    }
    # 文件字段
    files  = {'upload_file':open(file_path,'rb')}
    upload_url = 'http://192.168.2.160:80/services/api/advertise/Creatrreportinterface'
    response = requests.post(upload_url, data=upload_param,files=files)
    print(response.status_code)
    print(response.text)



if __name__ == "__main__":
    station_name='SINBUY_IT'
    path = r"C:\Users\Administrator\Desktop\非规范提取关键字_upload_2_erp_SINBUY_IT.xlsx"
    upload_files_2_server(station_name,path)