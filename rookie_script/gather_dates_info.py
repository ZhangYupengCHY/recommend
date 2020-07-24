import requests
import win32api
import os
import zipfile
from datetime import datetime
import datetime as dt
import numpy as np
import pandas as pd
import shutil
import re
import time


# 解压文件包
def unzip_dir(zip_dir):
    base_dir = zip_dir.split('.')[0]
    if not os.path.exists(base_dir):
        os.mkdir(base_dir)
    z = zipfile.ZipFile(zip_dir, "r")
    # 打印zip文件中的文件列表
    file_name_list = z.namelist()
    for filename in file_name_list:
        # 读取zip文件中的第一个文件
        content = z.read(filename)
        with open(base_dir + '/' + filename, 'wb') as f:
            f.write(content)
    return file_name_list


# 获得active,business,search这三个表的信息
def find_str(files_name):
    tmp_series = pd.Series([])
    check_reports = ['active', 'business', 'search']
    for report in check_reports:
        if (np.array([filename.find(report) for filename in files_name]) >= 0).any():
            tmp_series[report] = '完整'
        else:
            tmp_series[report] = '不完整'
    return tmp_series


# 获得日期字符串
def parse_date(file_name):
    file_date = re.findall('2019.[0-9]{2}.[0-9]{2}', file_name)
    file_date = file_date[0]
    if len(file_date) == 10:
        file_date = file_date[0:4] + '-' + file_date[5:7] + '-' + file_date[-2:]
    if len(file_date) == 8:
        file_date = file_date[0:4] + '-' + file_date[4:6] + '-' + file_date[-2:]
    return file_date


# 将时间的字符串格式转换成时间的集合
def parse_list_2_date(str_list):
    if len(str_list) > 0:
        sign_interval = str_list[0][4]
        date_list = [datetime.strptime(i, '%Y{}%m{}%d'.format(sign_interval, sign_interval)) for i in str_list]
    else:
        date_list = []
    return date_list


# 计算日期差
def diff_date():
    # camp表是提前两天，order是提前一天
    date_diff = (datetime.now() - datetime(2019, 8, 7)).days
    return date_diff


# 获得站点表是否缺失
def get_reports(file_name):
    camp_n_order, rest_reports = find_report(file_name), find_str(file_name)
    camp_n_order['Bulk_1'] = parse_list_2_date(camp_n_order['Bulk'])
    camp_n_order['all_orders_1'] = parse_list_2_date(camp_n_order['allorders'])
    if datetime.now() < datetime(2019, 9, 7):
        start_date = datetime(2019, 8, 7)
    else:
        start_date = datetime.now() - dt.timedelta(days=30)
    range_date_camp = list(pd.date_range(start_date, datetime.now()))[:-2]
    range_date_order = list(pd.date_range(start_date, datetime.now()))[:-1]
    range_date_camp = set([datetime(list_day.date().year,list_day.date().month,list_day.date().day) for list_day in range_date_camp])
    range_date_order = set([datetime(list_day.date().year,list_day.date().month,list_day.date().day) for list_day in range_date_order])
    camp_diff = list(range_date_camp - set(camp_n_order['Bulk_1']))
    order_diff = list(range_date_order - set(camp_n_order['all_orders_1']))
    camp_n_order['Bulk_1'] = [i.strftime('%Y%m%d') for i in camp_diff]
    camp_n_order['all_orders_1'] = [i.strftime('%Y%m%d') for i in order_diff]
    camp_n_order['station_name'] = station_name
    camp_n_order['station'] = station_name[0:-3]
    camp_n_order['country'] = station_name[-2:]
    all_info = pd.concat([camp_n_order[['station_name', 'station', 'country', 'Bulk_1', 'all_orders_1']], rest_reports])
    return all_info


# 找到camp与order文件
def find_report(files_name):
    # 将站点的camp,orders是否完整的信息形成一个list
    tmp_series = pd.Series([])
    check_reports = ['Bulk', 'allorders']
    for report in check_reports:
        files = [filename for filename in files_name if filename.find(report) >= 0]
        file_dates = [parse_date(file) for file in files]
        tmp_series[report] = file_dates
    return tmp_series


# 读取所有站点,同时处理无法请求的问题
def request_camps(camps_list):
    j = 1
    all_info = pd.Series()
    with open(r'C:\Users\Administrator\Desktop\{}_files_completed_info.txt'.format(today_date), 'a+') as f:
        f.write('station_name\tcamp\tcountry\tbulks\torders\tactives\tbussiness\tsearch\n')
        f.close()
    for camp in camps_list:
        times = 0
        try:
            one_info = request_ad(camp)
        except:
            times += 1
            time.sleep(2)
            print('正在请求,请稍等.')
            while 1:
                try:
                    one_info = request_ad(camp)
                    break
                except:
                    one_info = pd.Series()
                if times > 3:
                    break
        one_info = pd.Series(one_info)
        info_copy = one_info.copy()
        if len(info_copy) == 0:
            info_copy = pd.Series([station_name, station_name[0:-3], station_name[-2:], '该站点文件全部缺失', '该站点文件全部缺失', '该站点文件全部缺失', '该站点文件全部缺失', '该站点文件全部缺失'])
        with open(r'C:\Users\Administrator\Desktop\{}_files_completed_info.txt'.format(today_date), 'a+') as f:
            for i in info_copy:
                if isinstance(i, list):
                    i = ','.join(sorted(i))
                f.write(i)
                f.write('\t')
            f.write('\n')
        f.close()
    #     all_info = pd.concat([all_info, one_info], axis=1, sort=False)
    #     # print("目前正运行到第{}个,请耐心等待,还剩下{}个.".format(100*m+j, (len_camps-100*m-j)))
    #     j += 1
    # all_info = all_info.T[1:]
    # return all_info


# 读取一各站点下的所有文件
def request_ad(camp):
    global station_name
    download_url = "http://192.168.129.240:8080/ad_api/download"
    post_load = {'shop_station': '{}'.format(camp), 'passport': 'marmot'}
    response = requests.post(download_url, data=post_load).content
    data_url = response.decode()
    if 'http' not in data_url:
        return pd.DataFrame()
    # print(data_url)
    file_r = requests.get(response)
    status_code = file_r.status_code
    if status_code == 200:
        out_content = file_r.content  # {'data': file_r.content, 'msg': 'complete'}
    else:
        return pd.DataFrame()  # {'data': "", 'msg': status_code}
    zip_dir = 'C:/KMSOFT/Download/station_report/'
    if not os.path.exists(zip_dir):
        os.mkdir(zip_dir)
    now_date = datetime.now().date().strftime('%Y%m%d')
    station_name = camp
    base_dir = zip_dir + '{}'.format(camp).upper() + '_{}'.format(now_date)
    station_zip = base_dir + '.zip'
    with open(station_zip, 'wb') as f:
        f.write(out_content)
    file_list = unzip_dir(station_zip)
    zhandian_info = get_reports(file_list)
   #  shutil.rmtree(base_dir)
    os.remove(station_zip)
    return zhandian_info


# 读取站点信息
def read_account(path):
    account_info = pd.read_excel(path, sheet_name=0)
    account_name = account_info['account_num']
    new_accounts = []
    for i, j in zip(account_name, account_info['site']):
        if j == 'sp' or j == 'SP':
            j = 'es'
        new_account = i + '_' + j
        new_accounts.append(new_account)
    return new_accounts


if __name__ == '__main__':
    today_date = datetime.now().date()
    files_path = 'D:/all_stations_files/yibai_amazon_account.xlsx'
    all_camps_name = read_account(files_path)
    request_camps_name = all_camps_name[:2]
    request_camps(request_camps_name)
