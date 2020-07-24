# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/12/26 17:21
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import rsa, json, requests, os, shutil, redis
import difflib, xlsxwriter
import win32api
import glob
import numpy as np
import tkinter.messagebox
from tkinter import *
import pandas as pd
import random
from io import StringIO
# import Crypto.PublicKey.RSA
import base64, pymysql
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

THREAD_POOL = ThreadPoolExecutor(6)
PROCESS_POOL = ProcessPoolExecutor(2)
# saved_basename
save_dirname = "D:/month_files"
redis_pool_7 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=4, password='chy910624', decode_responses=True)
red = redis.StrictRedis(connection_pool=redis_pool_7)
now_date = str(datetime.now().date())
country_name_translate = {'加拿大': '_ca', '墨西哥': '_mx', "美国": '_us', '日本': '_jp', '印度': '_in', '澳大利亚': '_au', '德国': "_de",
                          '法国': '_fr', '意大利': '_it', '西班牙': '_es', '英国': '_uk'}


class Queue(object):
    # 定义一个空队列
    def __init__(self):
        self.items = []

    # 队列(只能在队尾)添加一个元素
    def enqueue(self, item):
        self.items.append(item)

    # 删除队列（只能在对头）一个元素
    def dequeue(self):
        self.items.pop(0)

    # 判断队列是否为空
    def isEmpty(self):
        return (self.items == [])

    # 清空队列
    def clear(self):
        del (self.items)  # 该队列就不存在了，而不是清空元素

    # 返回队列项的数量
    def size(self):
        return (len(self.items))


# 加载全部的站点名和广告专员
def db_download_station_names(db='team_station', table='only_station_info', ip='wuhan.yibai-it.com', port=33061,
                              user_name='marmot', password='') -> pd.DataFrame:
    """
    加载广告组接手的站点名
    :return: 所有站点名 'station_name', 'manger'
    """
    try:
        conn = pymysql.connect(
            host=ip,
            user=user_name,
            password=password,
            database=db,
            port=port,
            charset='UTF8')
    except Exception as e:
        print(f'CONNECT ERROR: cant connect  {table}')
        return
    # 创建游标
    cursor = conn.cursor()
    # 写sql
    sql = """SELECT station,ad_manger FROM {} """.format(table)
    # 执行sql语句
    cursor.execute(sql)
    stations_name_n_manger = cursor.fetchall()
    stations_name_n_manger = pd.DataFrame([list(station) for station in stations_name_n_manger],
                                          columns=['station_name', 'manger'])
    stations_name_n_manger.drop_duplicates(inplace=True)
    conn.commit()
    cursor.close()
    conn.close()
    return stations_name_n_manger


# 获取站点文件的请求路径
def get_all_files_dir(station_name, download_url="http://120.78.243.154/services/api/advertise/getreport"):
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
    station_name = station_name[0:-3] + '_' + station_name[-2:]

    def get_report(station_name):
        post_load = {
            'token': token,
            'data': json.dumps({
                0: {
                    'account_id': station_name,
                    "report_type": "CAM"
                },
                1: {
                    'account_id': station_name,
                    "report_type": "LBR"
                }
            })
        }

        response = requests.post(download_url, data=post_load).content
        data = []

        response_code = json.loads(response)['status']
        now_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%d')
        red_station_name = f'response code: {now_date}: {station_name}'
        red.rpush(red_station_name,*[response_code,now_datetime])
        data_basic = json.loads(response)['data']
        data.extend(data_basic)

        return data

    data = get_report(station_name)
    return data


# 读取单个文件数据(若为excel,则读取单个sheet)
def read_files(files_path: 'full_path', sheet_name='Sheet1'):
    split_file_path = os.path.splitext(files_path)
    if len(split_file_path) > 1:
        file_type = split_file_path[-1].lower()
        if file_type in ['.csv', '.txt']:
            try:
                file_data = pd.read_csv(files_path, error_bad_lines=False, warn_bad_lines=False)
                return file_data
            except Exception as e:
                file_data = pd.read_csv(files_path, encoding="ISO-8859-1", error_bad_lines=False, warn_bad_lines=False)
                return file_data
            except Exception as e:
                print(f"文件无法被正确读取: {files_path}")
        if file_type in ['.xlsx', '.xls']:
            try:
                if sheet_name == 'Sheet1':
                    file_data = pd.read_excel(files_path)
                    return file_data
                else:
                    read_excel = pd.ExcelFile(files_path)
                    sheet_names = read_excel.sheet_names
                    if sheet_name not in sheet_names:
                        print(f'{files_path}中没有{sheet_name}.')
                        return
                    else:
                        file_data = read_excel.parse(sheet_name)
                        return file_data
            except Exception as e:
                print(f"文件无法被正确读取:{files_path}")
        else:
            print(f'文件不为文本格式,请检查文件:{files_path}')
    else:
        print(f'请检查文件是否为有效路径:{files_path}')


# 请求并保存请求到的2种类型的报表
def request_month_files():
    # files_save_dirname = "D:/month_files"

    files_save_dirname = os.path.join(save_dirname, now_date)
    if stations_queue.size() == 0:
        return
    station_name = stations_queue.items[0]
    try:
        stations_queue.dequeue()
        print(station_name)
    except Exception as e:
        print(station_name, e)
        return
    print(stations_queue.size())
    if station_name:
        # print(f"START: {station_name} ")
        all_file_list = get_all_files_dir(station_name)
        if len(all_file_list) < 1:
            return

        # 通过api请求报表
        [download_from_api(request_dir, files_save_dirname, station_name) for request_dir in all_file_list]


# api请求报表存储到本地中
def download_from_api(api_dir: 'dir', files_save_dirname, station_name):
    station_name = station_name[:-3].lower() + '_' + station_name[-2:].lower()
    newest_dir = api_dir
    newest_dir = newest_dir.replace('/mnt/erp', 'http://120.78.243.154')
    file_basename = os.path.basename(newest_dir)
    try:
        request_file = requests.get(newest_dir)
    except Exception as e:
        print(e)
        print(f'{station_name}: 请求的链接{newest_dir}')
        return
    status_code = request_file.status_code
    if status_code == 200:
        # REDS load station cant request into red
        if 'campaign' in api_dir.lower():
            red.rpush(f'monthly_request_result: {now_date}:  campaigns_can_request', station_name)
        if 'business' in api_dir.lower():
            red.rpush(f'monthly_request_result: {now_date}:  business_can_request', station_name)

        out_content = request_file.content

        #  find manager who manage the station which api
        def match_station_name(api_request_station_name):
            '''
            通过api请求的站点名匹配到only_station_info 中的负责人姓名
            :param station_name: api请求的站点名
            :return: manager name
            '''
            # trans site sp to es
            if api_request_station_name.lower()[-2:] == 'sp':
                api_request_station_name = api_request_station_name[:-2] + 'es'
            # first: find 100 percent match
            # 　output format like (manager,station)
            if (manager_station is None) or (manager_station.empty):
                print('CANT CONNECT DATABASE')
                return
            # 1. complete matched
            manager_station_name = [manager for manager, ad_named_station in
                                    zip(manager_station['manger'], manager_station['station_name']) if
                                    api_request_station_name.lower() == ad_named_station.lower()]
            if len(manager_station_name) > 0:
                return [manager_station_name[0], api_request_station_name]
            else:
                red.rpush(f'monthly_request_result: {now_date}: api_request_station_name_cant_match_manager',
                          api_request_station_name)
                return ['Nobody',api_request_station_name]
            # 2. similarity station name rate (0.8)
            # manager_score = {ad_named_station: difflib.SequenceMatcher(None, ad_named_station.lower(),
            #                                                            api_request_station_name.lower()).quick_ratio()
            #                  for ad_named_station in manager_station['station_name']}
            # # find the most similar station and score must above 0.8,if manager not exist,give Nobody
            # similar_score = max(manager_score.values())
            # if similar_score < 0.8:
            #     print(f'Station name error: STATION {api_request_station_name} cant match a manager')
            #     red.rpush(f'monthly_request_result: {now_date}: api_request_station_name_cant_match_manager',
            #               api_request_station_name)
            #     return ['Nobody', api_request_station_name]
            # match_station = max(manager_score, key=manager_score.get)
            # red.rpush(f'monthly_request_result: {now_date}: matched_station:{api_request_station_name}',
            #           match_station)
            # manager = manager_station['manger'][manager_station['station_name'] == match_station]
            # return [manager.values[0], match_station]

        # save files by  manager and rename station
        [manager, matched_station_name] = match_station_name(station_name)
        station_name = matched_station_name
        file_dirname = os.path.join(files_save_dirname, manager, station_name)
        if not os.path.exists(file_dirname):
            os.makedirs(file_dirname)
        files_save_dirname = os.path.join(file_dirname, file_basename)
        with open(files_save_dirname, 'wb') as f:
            f.write(out_content)


def thread_read_file():
    while 1:
        all_task = []
        for one_page in range(1):
            all_task.append(THREAD_POOL.submit(request_month_files))
        for future in as_completed(all_task):
            future.result()
        if stations_queue.isEmpty():
            break


# 通过mac地址匹配获取请求的站点名
def get_stations():
    # 请求的站点数
    stations_path = r"D:\AD-Helper1\ad_helper\recommend\monthly_upload\完整站点.xls"
    stations_name = read_files(stations_path)
    stations_name = stations_name['站点']
    stations_name = [station.lower()[:-2] + 'sp' if 'es' in station.lower()[-3:] else station for station in
                     stations_name]
    stations_name = [station.lower()[:-2] + 'uk' if 'eu' in station.lower()[-3:] else station for station in
                     stations_name]
    # 将请求的站点写入到redis中
    if  stations_name:
        red.rpush(f'monthly_request_result: {now_date}:  whole_request_stations', *list(stations_name))
    # 将stations_name站点写入队列
    stations_queue = Queue()
    if len(stations_name) == 0:
        return
    for station in stations_name:
        stations_queue.enqueue(station)

    return stations_queue


# 请求月数据的主程序
def request_main():
    global stations_queue, manager_station
    # 1.1 clear today's redis keys
    red_old_keys = red.keys()
    [red.delete(key) for key in red_old_keys if now_date in key]
    # api请求的站点列表
    stations_queue = get_stations()
    # manager and station in table <only_station_info>
    manager_station = db_download_station_names()
    stations_num = stations_queue.size()
    #删除今天red所有的数据
    all_stations = stations_queue.items.copy()
    print(f"此次请求{stations_num}个站点.")
    # 删除历史文件夹
    files_save_dirname = os.path.join(save_dirname, now_date)
    if os.path.exists(files_save_dirname):
        shutil.rmtree(files_save_dirname)
    thread_read_file()
    print(f"{stations_num}个站点全部完成.")


# step2: calculator manager stations
def calc_managers_stations():
    def calc_one_manager_stations(manager_name):
        """
        use 'camp file' and 'br file' calculator one manager monthly data
        :param manager_name:manager name
        :return:grouped monthly data
        """
        manager_set = set(manager_station['manger'])
        if manager_name not in manager_set:
            return
        manager_basename = os.path.join(today_folder, manager_name)
        # step1: check folder exist or not
        if not os.path.exists(manager_basename):
            print(f'FOLDER DO NOT EXIST: {manager_name} monthly folder do not exist!')
            return
        # step2: check folder empty or not
        manager_files = os.listdir(manager_basename)
        manager_all_station = manager_station['station_name'][manager_station['manger'] == manager_name]
        # setp3 status1 station lost two files:
        stations_lost_two_files = set(manager_all_station) - set(manager_files)
        [red.rpush(f'monthly_request_result: {now_date}: {manager_name}: no_camp_monthly', station_name) for
         station_name in stations_lost_two_files]
        [red.rpush(f'monthly_request_result: {now_date}: {manager_name}: no_br_monthly', station_name)
         for station_name in stations_lost_two_files]
        if not manager_files:
            print(f'FOLDER IS EMPTY:{manager_basename} is empty!')
            return
        # step3 status2 station lost one file
        # step3.1:
        # lost camp file
        [red.rpush(f'monthly_request_result: {now_date}: {manager_name}: no_camp_monthly', station_name) for
         station_name in
         manager_files if
         (len(os.listdir(os.path.join(manager_basename, station_name))) == 1) & (
                 'campaign' not in ''.join(os.listdir(os.path.join(manager_basename, station_name))).lower())]
        # lost br file
        [red.rpush(f'monthly_request_result: {now_date}: {manager_name}: no_br_monthly', station_name) for station_name
         in
         manager_files if
         (len(os.listdir(os.path.join(manager_basename, station_name))) == 1) & (
                 'businessreport' not in ''.join(os.listdir(os.path.join(manager_basename, station_name))).lower())]

        # [shutil.rmtree(os.path.join(manager_basename, station_name)) for station_name in manager_files if
        #  len(os.listdir(os.path.join(manager_basename, station_name))) != 2]

        # step4 : calculator the manager
        def calc_manager(folder_path, manager_name):
            if manager_name == '汪磊':
                return
            now_month = datetime.now().month
            if now_month != 1:
                month_flag = now_month - 1
            else:
                month_flag = 12
            year_flag = datetime.now().year
            # every station country get data
            df_br = pd.DataFrame()
            df_br_all = pd.DataFrame()
            df_cp = pd.DataFrame()
            df_cp_all = pd.DataFrame()
            df_ad_product = pd.DataFrame()
            df_ad_product_all = pd.DataFrame()

            # error_file_dir = os.path.join(os.path.dirname(folder_path), 'error_msg.txt')

            error_flag = 0
            i = 0
            for name in os.listdir(folder_path):
                # account, country = (name.split("~")[0], name.split("~")[1].upper())
                '''
                account = re.findall('[A-Za-z0-9]+', name)[0].lower()
                country = re.findall('[A-Za-z0-9]+', name)[1].upper()
                '''
                account = name[:-3].lower()
                country = name[-2:].upper()
                station_name = account + '_' + country
                files_path = folder_path + r'\\' + name
                os.chdir(files_path)
                # 读取Br表
                for file in glob.glob('*BusinessReport*.csv'):
                    # can read br or not
                    try:
                        df_br = pd.DataFrame(pd.read_csv(file, engine='python', encoding='utf_8_sig'))
                    except Exception as e:
                        red.rpush(
                            f'monthly_request_result: {now_date}: {manager_name}: br_monthly_data_is_error',
                            station_name)
                        error_flag = 1
                        continue
                    df_br.rename(columns={"日期": "Date",
                                          "已订购商品销售额": "Ordered Product Sales",
                                          "已订购商品的销售额 – B2B": "Ordered Product Sales – B2B",
                                          "订购数量 – B2B": "Units Ordered – B2B",
                                          "订单商品种类数": "Total Order Items",
                                          "订单商品总数 – B2B": "Total Order Items – B2B",
                                          "页面浏览次数": "Page Views",
                                          "买家访问次数": "Sessions",
                                          "购买按钮赢得率": "Buy Box Percentage",
                                          "订单商品数量转化率": "Unit Session Percentage",
                                          "商品转化率 – B2B": "Unit Session Percentage – B2B",
                                          "平均在售商品数量": "Average Offer Count",
                                          "平均父商品数量": "Average Parent Items"}, inplace=True)
                    if 'Date' not in (df_br.columns):
                        red.rpush(f'monthly_request_result: {now_date}: {manager_name}: br_monthly_data_is_error',
                                  station_name)
                        error_flag = 1
                        continue
                    df_br = df_br.reindex(columns=['Date', 'Ordered Product Sales', 'Units Ordered', 'Sessions'])
                    df_br['account'] = account
                    df_br['country'] = country
                    df_br_all = df_br_all.append(df_br)
                # print(account + "_" + country + ",Business Report Done!!!")

                # 读取广告报表
                for file in glob.glob('*CAMPAIGN*'):
                    if file[-3:] == 'csv':
                        try:
                            df_cp = pd.DataFrame(pd.read_csv(file, engine='python', encoding='utf_8_sig'))
                        except:
                            try:
                                df_cp = pd.DataFrame(pd.read_csv(file, engine='python', encoding='ANSI'))
                            except:
                                red.rpush(
                                    f'monthly_request_result: {now_date}: {manager_name}: camp_monthly_data_is_error',
                                    station_name)
                                continue

                    else:
                        df_cp = pd.DataFrame(pd.read_excel(file))
                    df_cp['account'] = account
                    df_cp['country'] = country
                    # campaign 表头标准化
                    df_cp.rename(columns=lambda x: re.sub('\\(.*?\\)|\\{.*?}|\\[.*?]', '', x), inplace=True)
                    df_cp.rename(
                        columns={'状态': 'State', '广告活动': 'Campaigns', '状态.1': 'Type', '类型': 'Status', '投放': 'Targeting',
                                 '广告活动的竞价策略': 'Campaign bidding strategy', '开始日期': 'Start date', '结束日期': 'End date',
                                 '广告组合': 'Portfolio',
                                 '预算': 'Budget', '曝光量': 'Impressions', '点击次数': 'Clicks', '点击率 ': 'CTR', '花费': 'Spend',
                                 '每次点击费用 ': 'CPC', '订单': 'Orders', '销售额': 'Sales', '广告投入产出比 ': 'ACos'}, inplace=True)
                    if 'State' not in (df_cp.columns):
                        red.rpush(f'monthly_request_result: {now_date}: {manager_name}:  camp_monthly_data_is_error',
                                  station_name)
                        error_flag = 1
                        continue
                    df_cp = df_cp.reindex(
                        columns=['State', 'Campaigns', 'Status', 'Type', 'Targeting', 'Start date', 'End date',
                                 'Budget',
                                 'Impressions', 'Clicks', 'CTR', 'Spend', 'CPC', 'Orders', 'Sales', 'ACoS', 'account',
                                 'country'])
                    df_cp_all = df_cp_all.append(df_cp)
                # print(account + "_" + country + ",Campaign Done!!!")

                # 读取 Advertised product表
                '''
                if not glob.glob(files_path + '/*Advertised product*'):
                    message = f'{station_name}缺失Advertised product表.'
                    i += 1
                    with open(error_file_dir, mode="a+") as f:
                        f.write(f"{i}:{message}" + "\n")
                for file in glob.glob('*Advertised product*'):
                    try:
                        df_ad_product = pd.read_excel(file)
                    except Exception as e:
                        print(f'{file}表有问题，请查看.')
                        print(e)
                    df_ad_product['account'] = account
                    df_ad_product['country'] = country
                    station_name = account + '_' + country
                    df_ad_product['station'] = station_name
                    df_ad_product['year'] = year_flag
                    df_ad_product['month'] = month_flag
                    if df_ad_product.empty:
                        continue
                    # df_cp = df_cp.reindex(
                    #     columns=['State', 'Campaigns', 'Status', 'Type', 'Targeting', 'Start date', 'End date', 'Budget',
                    #              'Impressions', 'Clicks', 'CTR', 'Spend', 'CPC', 'Orders', 'Sales', 'ACoS', 'account',
                    #              'country'])
                    df_ad_product = init_ad_product(df_ad_product)
                    db_upload_ad_product(df_ad_product)
                    '''

            '''
            if os.path.exists(error_file_dir):
                with open(f"{error_file_dir}", "a+") as f:  # 打开文件
                    f.write("==================================" + '\n')
                    f.write("请下载完整表格后重新运行上传程序。")
                with open(f"{error_file_dir}", "r") as f:  # 打开文件
                    data = f.read()
                tkinter.messagebox.showinfo(message=data)
                return
            '''


            # business report ,月份/年份/货币金额
            def str2month(df):
                if len(re.findall('[0-9]+', df['Date'])[0]) == 4:
                    return re.findall('[0-9]+', df['Date'])[1]
                else:
                    if df['country'] in ['US', 'CA', 'JP', 'MX']:
                        return re.findall('[0-9]+', df['Date'])[0]
                    else:
                        return re.findall('[0-9]+', df['Date'])[1]

            def str2year(df):
                if len(re.findall('[0-9]+', df['Date'])[0]) == 4:
                    return re.findall('[0-9]+', df['Date'])[0]
                else:
                    return re.findall('[0-9]+', df['Date'])[2]

            if df_br_all.empty:
                return
            df_br_all = df_br_all.reset_index(drop=True)
            df_br_all['month'] = df_br_all.apply(lambda x: str2month(x), axis=1)
            df_br_all['year'] = df_br_all.apply(lambda x: str2year(x), axis=1)
            df_br_all['month'] = df_br_all['month'].astype('int')
            df_br_all['year'] = df_br_all['year'].astype('int')
            df_br_all['Ordered Product Sales'] = df_br_all['Ordered Product Sales'].str.extract('(\d+,?\d*.\d+)')
            for col in ['Ordered Product Sales', 'Units Ordered', 'Sessions']:
                df_br_all[col] = df_br_all[col].astype('str')
                df_br_all[col] = df_br_all[col].str.replace(',', '').astype('float')
            df_br_month = df_br_all[(df_br_all['month'] == month_flag) & (df_br_all['year'] == year_flag)]

            # campaign, 直接汇总生成
            def money2num(num):
                num = num.rstrip()
                if any(i in num for i in [',', '.']):  # 原数据中含有,.等符号
                    res = ''
                    for ii in filter(str.isdigit, num):
                        res += ii
                    if num[-3].isdigit():
                        return float(res) / 10
                    else:
                        return float(res) / 100
                else:
                    return float(num + '00') / 100

            def amount2num(num):
                res = ''
                for ii in filter(str.isdigit, num.split('.')[0]):
                    res += ii
                return int(res)
            if df_cp_all.empty:
                return
            df_cp_all.dropna(subset=['Spend'], inplace=True)
            for col in ['Spend', 'Sales']:
                df_cp_all[col] = df_cp_all[col].astype('str')
                df_cp_all[col] = df_cp_all[col].apply(lambda x: money2num(x))

            for col in ['Clicks', 'Orders']:
                df_cp_all[col] = df_cp_all[col].astype('str')
                df_cp_all[col] = df_cp_all[col].apply(lambda x: amount2num(x))
            df1 = df_cp_all.groupby(['account', 'country'])['Clicks', 'Spend', 'Orders', 'Sales'].sum().reset_index()
            df1['ACoS'] = df1['Spend'] / df1['Sales']
            df1['CPC'] = df1['Spend'] / df1['Clicks']
            df1['CR'] = df1['Orders'] / df1['Clicks']

            # if df_br_month is None:
            #     return
            # business merge campaign
            df_merge = df_br_month.merge(df1, on=['account', 'country'], how='outer')
            df_merge['account'] = df_merge['account'].str.upper()
            df_merge['month'] = str(month_flag) + '月'
            df_merge['year'] = str(year_flag) + '年'
            df_merge['account-country'] = df_merge['account'] + '-' + df_merge['country']
            df_merge['spend/sales'] = df_merge['Spend'] / df_merge['Ordered Product Sales']
            df_merge['Clicks/Sessions'] = df_merge['Clicks'] / df_merge['Sessions']
            df_merge['order percentage'] = df_merge['Orders'] / df_merge['Units Ordered']
            df_merge['session percentage'] = df_merge['Units Ordered'] / df_merge['Sessions']
            df_merge = df_merge.replace([np.inf], 0)
            df_merge = df_merge.reindex(
                columns=['account', 'country', 'account-country', 'year', 'month', 'Spend', 'ACoS', 'Sales',
                         'Ordered Product Sales', 'spend/sales', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                         'Clicks', 'Sessions', 'Clicks/Sessions', 'Orders', 'Units Ordered',
                         'order percentage', 'CR', 'session percentage'])
            os.chdir(folder_path[:folder_path.rfind('\\')])
            save_path = 'alldata' + f'{year_flag}年' + str(month_flag) + '月-' + manager_name + '.xlsx'
            df_merge.to_excel(save_path, index=False)

            # 上传月报表到数据库中station_statistic
            def upload_monthly_data(save_path):
                url = 'http://wuhan.yibai-it.com:8888/analysis/api/upload/month/'
                # url = 'http://127.0.0.1:8888/analysis/api/upload/month/'
                file_name = os.path.basename(save_path)
                files = {
                    'file': (file_name, open(save_path, 'rb')),
                }
                resp = requests.post(url, files=files,data={'real_name': manager_name})  # 上传文件
                if resp.text == 'fail':
                    print(f'cant upload to database fail: {manager_name}')
                if resp.text == 'ok':
                    print(f'upload to database successful: {manager_name} ')

            # upload_monthly_data(save_path)


        calc_manager(manager_basename, manager_name)


    # 1 find completed manager
    today_folder = os.path.join(save_dirname, now_date)

    # 2.1:check today folder exist or not
    if not os.path.exists(today_folder):
        print(f'FOLDER DO NOT EXIST: {today_folder} do not exist!')
        return

    # 2.2: check folder is empty or not
    managers_list = os.listdir(today_folder)
    if not managers_list:
        print(f'FOLDER IS EMPTY:{today_folder} is empty!')

    # 2.3 calculator manager station:camp and br
    [calc_one_manager_stations(manager) for manager in managers_list]

    # 2.4 manager daily log
    red_all_keys = red.keys()
    manager = manager_station['manger']
    manager_set = set(manager) - set(['汪磊', '王艳', '张于鹏'])
    all_manager_key = [{manager: key} for manager in manager_set for key in red_all_keys if
                       (now_date in key) and (manager in key)]
    save_log_path = os.path.join(today_folder, f'monthly_data_{now_date}.xlsx')
    if os.path.exists(save_log_path):
        os.remove(save_log_path)

    # 3. 创建一个workbook 设置编码

    def get_other_company_station(path):
        station_list = pd.read_excel(path)['广告后台店铺名']
        station_list = [station.replace('-', '_').replace(' ', '_').lower() for station in station_list]
        return station_list

    # 非易佰站点名
    path = r"D:\AD-Helper1\ad_helper\recommend\monthly_upload\非易佰站点.xlsx"
    other_company_station = get_other_company_station(path)

    # 冻结的站点
    fz_stations = pd.read_excel(r"D:\AD-Helper1\ad_helper\recommend\monthly_upload\冻结站点.xlsx")
    fz_stations = fz_stations['冻结站点'].apply(lambda x: x.lower())

    workbook = xlsxwriter.Workbook(save_log_path)
    # 3.1 总的站点情况
    worksheet_all = workbook.add_worksheet('总数据情况')
    if (all_manager_key is None) or (not all_manager_key):
        return
    br_lost = sum(
        [red.lrange(list(key.values())[0], 0, -1) for key in all_manager_key if
         'no_br_monthly' in list(key.values())[0]], [])
    camp_lost = sum(
        [red.lrange(list(key.values())[0], 0, -1) for key in all_manager_key if
         'no_camp_monthly' in list(key.values())[0]], [])
    camp_data_error = sum([red.lrange(list(key.values())[0], 0, -1) for key in all_manager_key if
                           'camp_monthly_data_is_error' in list(key.values())[0]], [])
    br_data_error = sum([red.lrange(list(key.values())[0], 0, -1) for key in all_manager_key if
                         'br_monthly_data_is_error' in list(key.values())[0]], [])
    error_sign = {'br无法请求到的站点': br_lost, 'camp无法请求到的站点': camp_lost, 'br表错误的站点': br_data_error,
                  'camp表错误的站点': camp_data_error}

    i = 0
    for name, stations in error_sign.items():
        if stations:
            worksheet_all.write(0, i, name)
            worksheet_all.write(0, i+1, 'response_code/request_datetime')
            row = 0
            for station in stations:
                if (station.lower() in other_company_station) or (station.lower() in fz_stations):
                    continue
                worksheet_all.write(row + 2, i, station)
                # 写入站点的请求状态和请求时间
                station = station.lower()
                if station[-2:] == 'es':
                    station = station[:-2] + 'sp'

                def get_station_respon_code(station_name):
                    station_name_temp = station_name.lower()

                    if station_name_temp[-2:] == 'es':
                        station_name_temp = station_name_temp[:-2] + 'sp'
                    # 1. complete matched
                    today_red_keys = [key for key in red_all_keys if now_date in key]
                    station_match_key = [key for key in today_red_keys if (station_name_temp in key.lower()) & (now_date in key)]
                    if len(station_match_key) > 0:
                        station_match_key = station_match_key[0]
                        return '/'.join(red.lrange(station_match_key,0,-1))
                    # 2. similarity station name rate (0.8)
                    # match same site
                    # today_same_site_red_keys = [key for key in today_red_keys if  key.lower()[-2:] == station[-2:].lower()]
                    # if not today_same_site_red_keys:
                    #     return 'no response'
                    # station_key_score = {key: difflib.SequenceMatcher(None, station_name_temp,
                    #                                                            key.lower().split(':')[-1]).quick_ratio() for key in today_same_site_red_keys}
                    # # find the most similar station and score must above 0.8,if manager not exist,give Nobody
                    # similar_score = max(station_key_score.values())
                    # if similar_score < 0.85:
                    #     return 'no response'
                    # station_match_key = max(station_key_score,key=station_key_score.get)
                    # return '/'.join(red.lrange(station_match_key,0,-1))
                    else:
                        return 'no response'

                station_response_code = get_station_respon_code(station)

                worksheet_all.write(row + 2, i+1, station_response_code)
                row += 1
            worksheet_all.write(1, i, f'汇总: {row}')

            i += 2

    # 3.2 每个人的站点情况 创建一个worksheet
    worksheet = workbook.add_worksheet('负责人月数据情况')
    if (all_manager_key is None) or (not all_manager_key):
        return
    for columns, manager_key in enumerate(all_manager_key):
        manager_name = list(manager_key.keys())[0]
        manager_error_type = list(manager_key.values())[0]
        error_type_name = manager_error_type.split(':')[-1]
        manager_error_stations = red.lrange(manager_error_type, 0, -1)
        # 写到日志中
        worksheet.write(0, columns, manager_name)
        worksheet.write(1, columns, error_type_name)
        worksheet.write(2, columns, f'汇总: {len(manager_error_stations)}')
        for row, station in enumerate(manager_error_stations):
            worksheet.write(row + 3, columns, station)
    workbook.close()


if __name__ == '__main__':
    start_datetime = datetime.now()
    #  step 1: request  all stations monthly files into local disk
    request_main()
    # step 2: calculator manager stations
    calc_managers_stations()
    finish_datetime = datetime.now()
    print(finish_datetime)
    print(f'TOTAL COST {finish_datetime - start_datetime}')
