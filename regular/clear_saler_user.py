# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/6/17 15:20
@Author: RAMSEY

"""
from sqlalchemy import create_engine
import pandas as pd



engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
    'user', '', 'wuhan.yibai-it.com', 33061, 'team_station', 'utf8'))


def read_table(sql):
    # 执行sql
    conn = engine.connect()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df



def clear_saler_user(saler_staff_path = r"D:\AD-Helper1\ad_helper\recommend\regular\亚马逊姓名工号20200617.xlsx"):
    """
    删除掉销售注册表中职位为销售,不在职并且没有上传表记录()的账号
    Args:
        saler_staff_path:str
            在职人员名单

    Returns:None

    """
    saler_user_sql  = "SELECT * FROM saler_login"
    saler_user_data = read_table(saler_user_sql)
    saler_stay_path = saler_staff_path
    saler_stay_data = pd.read_excel(saler_stay_path)
    saler_staff_set = set(saler_stay_data['姓名'])
    saler_login = saler_user_data[saler_user_data['position']=='站点销售']
    leave_saler = saler_login[~saler_login['name'].isin(saler_staff_set)]
    # 加载销售上传文件日志名单
    saler_upload_file = "SELECT * FROM saler_upload_files_log"
    saler_upload_file_data = read_table(saler_upload_file)
    saler_upload_file_staff_set = set(saler_upload_file_data['username'])
    #
    leave_saler = leave_saler[~leave_saler['username'].isin(saler_upload_file_staff_set)]
    leave_saler.to_excel(r"C:\Users\Administrator\Desktop\销售端后台中未知姓名名单.xlsx",index=False)


if __name__ == '__main__':
    clear_saler_user()