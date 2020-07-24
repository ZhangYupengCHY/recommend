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

import rsa, json, requests, os, redis, zipfile, shutil, time, re, xlsxwriter
# from retry import retry
import pandas as pd
# import Crypto.PublicKey.RSA
import base64, pymysql
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

THREAD_POOL = ThreadPoolExecutor(6)
PROCESS_POOL = ProcessPoolExecutor(2)
redis_pool_6 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=6, decode_responses=True)
redis_pool_7 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=7, decode_responses=True)
red = redis.StrictRedis(connection_pool=redis_pool_7)
red_station_status = redis.StrictRedis(connection_pool=redis_pool_6)

country_name_translate = {'加拿大': '_ca', '墨西哥': '_mx', "美国": '_us', '日本': '_jp', '印度': '_in', '澳大利亚': '_au', '德国': "_de",
                          '法国': '_fr', '意大利': '_it', '西班牙': '_es', '英国': '_uk'}




# 加载全部的站点名和广告专员
def db_download_station_names(db='team_station', table='only_station_info', ip='wuhan.yibai-it.com', port=33061,
                              user_name='marmot', password='') -> pd.DataFrame:
    """
    加载广告组接手的站点名
    :return: 所有站点名
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
    sql = """SELECT station,ad_manger FROM {} """.format(table)
    # 执行sql语句
    cursor.execute(sql)
    stations_name_n_manger = cursor.fetchall()
    stations_name_n_manger = pd.DataFrame([list(station) for station in stations_name_n_manger],
                                          columns=['station_name', 'manger'])
    stations_name_n_manger = stations_name_n_manger[(stations_name_n_manger['manger']!='汪磊') & (stations_name_n_manger['manger']!='王艳')]
    stations_name_n_manger.drop_duplicates(inplace=True)
    conn.commit()
    cursor.close()
    conn.close()
    return stations_name_n_manger


def get_all_files_dir(station_name, key_path=r"E:\ad_zyp\api_request_files\public.key",
                      download_url="http://120.78.243.154/services/api/advertise/getreport"):
    with open(key_path, 'r') as fp:
        public_key = fp.read()
    # pkcs8格式
    key = public_key
    password = "Kr51wGeDyBM39Q0REVkXn4lW7ZqCxdPLS8NO6iIfubTJcvsjt2YpmAgzHFUoah"
    pubkey = rsa.PublicKey.load_pkcs1_openssl_pem(key)
    password = password.encode('utf8')
    crypt_password = rsa.encrypt(password, pubkey)
    token = base64.b64encode(crypt_password).decode()
    station_name = station_name[0:-3].replace('_', '-') + station_name[-3:]

    def get_report(station_name):
        post_load = {
            'token': token,
            'data': json.dumps({
                0: {
                    'date_range': 1,
                    "child_type": 30,
                    'account_id': station_name,
                    "report_type": "Campaign"
                },
                1: {
                    'date_range': 1,
                    'account_id': station_name,
                    "report_type": "ST"
                },
                2: {
                    'date_range': 1,
                    'account_id': station_name,
                    "report_type": "BR"
                },
                3: {
                    'date_range': 30,
                    "child_type": 30,
                    "account_id": station_name,
                    "report_type": "AO"
                },
                4: {
                    "account_id": station_name,
                    "report_type": "Active"
                },
                5: {
                    "account_id": station_name,
                    "report_type": "All"
                },
            })
        }
        response = requests.post(download_url, data=post_load).content
        data = []
        try:
            data_basic = json.loads(response)['data']
            data.extend(data_basic)
        except:
            pass
            # red.rpush('station_no_data', station_name)
            # return
        # 单独请求四天的广告报表
        post_camp_date = [1, 7, 14, 60]
        all_camp_post_dir = []
        for post_date in post_camp_date:
            post_camp_load = {
                'token': token,
                'data': json.dumps({
                    0: {
                        'date_range': 1,
                        "child_type": post_date,
                        'account_id': station_name,
                        "report_type": "Campaign"
                    }
                })
            }
            response_camp = requests.post(download_url, data=post_camp_load).content
            try:
                camp_data = json.loads(response_camp)['data']
            except:
                print(f"{station_name}广告报表{post_date}无法请求..")
                continue
            all_camp_post_dir.extend(camp_data)
        # data = all_camp_post_dir

        # 本地接口请求AO
        '''
        local_url = 'http://192.168.9.167:8080/services/api/advertise/getreport'
        post_ao_load = {
            'token': token,
            'data': json.dumps({
                0: {
                    'date_range': 30,
                    "child_type": 30,
                    'account_id': station_name,
                    "report_type": "AO"
                }
            })
        }
        response_ao = requests.post(local_url,data=post_ao_load).content
        try:
            ao_data = json.loads(response_ao)['data']
            ao_data = [data.replace('D:/phpStudy/PHPTutorial/WWW/wwwerp','http://192.168.9.167:8080') for data in ao_data]
        except:
            print(f"{station_name}:AO报表{post_date}本地无法请求..")
            ao_data = []
        data.extend(ao_data)
        '''

        data.extend(all_camp_post_dir)
        return data

    data = get_report(station_name)
    if (not data) & (station_name[-2:] == 'es'):
        station_name = station_name[:-2] + 'sp'
        data = get_report(station_name)
    if not data:
        station_name = station_name[0:-3].replace('-', ' ') + station_name[-3:]
        data = get_report(station_name)


    # print(data)
    if not data:
        red.rpush('station_no_files',station_name)
        return
    files_keyword_dict = {'ST': 'SearchTerm', 'BR': 'Business', 'AO': 'ORDER', 'AC': 'AVTIVE_LISTING',
                          'AL': 'All_LISTING'}
    camp_keyword_dict = ['Advertising', 'Sponsored']
    all_files_dict = {}
    for report_type, report_kw in files_keyword_dict.items():
        all_files_dict[report_type] = [report for report in data if files_keyword_dict[report_type] in report]
    all_files_dict['CP'] = [report for report in data if
                            (camp_keyword_dict[0] in report) or (camp_keyword_dict[1] in report)]
    return all_files_dict


# 保留所有站点最新的数据，剔除重复数据
def keep_newest_file_dir(all_files_dict: 'dict', station_name):
    file_keys = all_files_dict.keys()
    for report_type in file_keys:
        report_type_files = all_files_dict[report_type]
        if report_type == 'ST':
            continue
        if len(report_type_files) > 1:
            try:
                files_date = [re.findall('[0-9]{4}.[0-9]{2}.[0-9]{2}', os.path.basename(file)) for file in
                              report_type_files]
                # 排除没有日期的链接
                if not files_date[0]:
                    continue
                last_date = max([max(dates) for dates in files_date])
                all_files_dict[report_type] = [file for file in report_type_files if
                                               last_date in os.path.basename(file)]
            except:
                print(f"{station_name}有文件命名有问题.")
                pass
    return all_files_dict


# 通过接口获取可以请求的站点数
def get_request_stations(key_path=r"E:\ad_zyp\api_request_files\public.key",
                         download_url="http://192.168.9.167:8080/services/api/advertise/getstation"):
    with open(key_path, 'r') as fp:
        public_key = fp.read()
    # pkcs8格式
    key = public_key
    password = "Kr51wGeDyBM39Q0REVkXn4lW7ZqCxdPLS8NO6iIfubTJcvsjt2YpmAgzHFUoah"
    pubkey = rsa.PublicKey.load_pkcs1_openssl_pem(key)
    password = password.encode('utf8')
    crypt_password = rsa.encrypt(password, pubkey)
    token = base64.b64encode(crypt_password).decode()
    post_load = {
        'token': token}
    response = requests.post(download_url, data=post_load).content
    requests_response = json.loads(response)
    stations_name = [i['account_name'] for i in requests_response['data']]
    return stations_name


# 得到站点文件请求情况.
def request_log(station_folder):
    files_match_kw_dict = {'camp_30': '30天-bulksheet', 'order': 'order', 'br': 'business',
                           'st': 'search', 'al': 'all+listing', 'ac': 'active+listing', 'camp_1': '1天-bulksheet',
                           'camp_7': '7天-bulksheet', 'camp_14': '14天-bulksheet', 'camp_60': '60天-bulksheet'}
    requested_files = os.listdir(station_folder)
    stations_name = os.path.basename(station_folder)
    file_result_columns = ['站点名', '完整情况', '缺失文件数量']
    file_result_columns.extend(files_match_kw_dict.keys())
    station_file_result = pd.Series([''] * len(file_result_columns), index=file_result_columns)
    station_file_result['站点名'] = stations_name
    for file_type in files_match_kw_dict.keys():
        matched_file = [file for file in requested_files if files_match_kw_dict[file_type] in file.lower()]
        if matched_file:
            station_file_result[file_type] = '存在'
        else:
            station_file_result[file_type] = '不存在'
    lost_num = len(station_file_result[station_file_result == '不存在'])
    if lost_num == 0:
        station_file_result['完整情况'] = '完整'
    else:
        station_file_result['完整情况'] = '不完整'
    station_file_result['缺失文件数量'] = lost_num
    station_file_result = list(station_file_result)
    return station_file_result


# 检测站点站点请求的完整性
def request_file_complete_detect(station_file_path: 'full_dir', station_name):
    try:
        requested_files = os.listdir(station_file_path)
    except:
        return

    six_report_sign_words = {'1天': 'camp_1', '7天': 'camp_7', '14天': 'camp_14', '30天': "camp_30", '60天': 'camp_60',
                             'active+listing': 'active_listing',
                             'all+listing': 'all_listing', 'all orders': 'all_orders', 'business': 'business',
                             'search term report(last_month)': 'search_term_last_month'}
    if requested_files:
        files_add_str = ''.join(requested_files).lower()
        all_files_sign_word = six_report_sign_words.keys()
        missed_type = [six_report_sign_words[sign_words] for sign_words in all_files_sign_word if
                       sign_words not in files_add_str]
        # 本月的搜索词报表需要单独处理
        search_term = re.findall('search term report-', files_add_str)
        if not search_term:
            missed_type.extend(['search_term_this_month'])
        if missed_type:
            missed_type_str = ','.join(missed_type)
            red_station_status.set(station_name, missed_type_str)
        else:
            red.rpush('complete_files_station', station_name)
            red.rpush('complete_files_station_backup', station_name)
    else:
        red_station_status.set(station_name, 'requested_none')
    print(f'FINISH: {station_name}')


def gather_requests_result():
    not_complete_stations = red_station_status.keys()
    complete_station = red.lrange('complete_files_station_backup', 0, -1)
    file_sign_dict = {'camp_1': '成功推送广告报表1天', 'camp_7': '成功推送广告报表7天', 'camp_14': '成功推送广告报表14天',
                      'camp_30': '成功推送广告报表30天',
                      'camp_60': '成功推送广告报表60天', 'active_listing': '成功推送active_listing报表',
                      'all_listing': '成功推送all_listing报表', 'all_orders': '成功推送订单报表',
                      'business': '成功推送业务报表', 'search_term_this_month': '搜索词当月1号至今',
                      'search_term_last_month': '搜索词上月数据'}

    now_date = datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
    save_log_path = f'F:/api_request_all_files/request_log_{now_date}.xlsx'
    # 创建一个workbook 设置编码
    workbook = xlsxwriter.Workbook(save_log_path)
    # 创建一个worksheet
    worksheet = workbook.add_worksheet('站点请求结果')

    # 写入excel
    # 参数对应 行, 列, 值
    worksheet.write(0, 0, '时间')
    worksheet.write(1, 0, now_date)
    worksheet.write(0, 1, '推送过来完整数据账号')
    worksheet.write(1, 1, len(complete_station))
    i = 2
    for station in complete_station:
        worksheet.write(i, 1, station)
        i += 1

    stations_lost_files = {station: red_station_status.get(station).split(',') for station in not_complete_stations}
    num = 2
    for file_type_sign_word, file_type_name in file_sign_dict.items():
        file_type_success_stations = [station for station in not_complete_stations if
                                      file_type_sign_word not in stations_lost_files[station]]
        file_type_success_stations.extend(complete_station)
        type_len = len(file_type_success_stations)
        worksheet.write(0, num, file_type_name)
        worksheet.write(1, num, type_len)
        i = 2
        for station in file_type_success_stations:
            worksheet.write(i, num, station)
            i += 1
        num += 1

    # 添加请求的全部站点列
    all_request_stations =  red.lrange('requests_stations_name_backup', 0, -1)
    columns_num = len(file_sign_dict) + 2
    worksheet.write(0, columns_num, '请求的全部站点')
    worksheet.write(1, columns_num, len(all_request_stations))
    j = 2
    for station in all_request_stations:
        worksheet.write(j, columns_num, station)
        j += 1

    workbook.close()


# 请求并保存请求到的6种类型的报表
def request_save_all_6_files(files_save_dirname=r"F:\api_request_all_files"):
    now_date = str(datetime.now().date())
    files_save_dirname = os.path.join(files_save_dirname, now_date)
    station_name = red.lpop('requests_stations_name')
    if station_name:
        print(f"START: {station_name} ")
        all_file_dict = get_all_files_dir(station_name)
        if not all_file_dict:
            return
        all_file_key = all_file_dict.keys()
        all_file_dict = keep_newest_file_dir(all_file_dict, station_name)

        station_save_folder = os.path.join(files_save_dirname, station_name)
        if os.path.exists(station_save_folder):
            shutil.rmtree(station_save_folder)

        # 规范命名
        def unified_reports_name(files_folder: 'dirname', station_name) -> dict:
            '''
            广告报表    :账号-国家-30（7/14/30/60）天-bulksheet-月-日-年
            搜索词报告  :Sponsored Products Search term report-月-日-年
            业务报告    :BusinessReport-月-日-年
            在售商品报告:Active+Listings+Report+月-日-年
            全部商品报告:All+Listings+Report+月-日-年
            订单报告    :All Orders-月-日-年
            :param all_file_dict:
            :return:
            '''
            try:
                all_report_files = os.listdir(files_folder)
            except:
                return
            account = station_name[:-3]
            site = station_name[-2:]
            date = datetime.now().strftime('%m-%d-%Y')
            if not all_report_files:
                return
            report_sign_word = {'sevendays': 7, 'fourteendays': 14, 'bulknearlyamonth': 30, 'sixtydays': 60,
                                'amazonsponsoredproductsbulk': 1, 'amazonsearchtermreportmonthtodate': '(last_month)',
                                'amazonsearchtermreport': '', 'business': '', 'all_listing': '', 'avtive_listing': '',
                                'order': ''}
            # 广告报表改名字典
            report_sign_word = {key: f'{account}-{site}-{value}天-bulksheet-{date}' if ('day' in key.lower()) or (
                    'bulk' in key.lower()) else value for key, value in
                                report_sign_word.items()}
            # 搜索词改名字典
            report_sign_word = {
                key: f'Sponsored Products Search term report{value}-{date}' if ('search' in key.lower()) else value for
                key, value in
                report_sign_word.items()}
            # 业务报表改名字典
            report_sign_word = {key: f'BusinessReport-{date}' if
            'business' in key.lower() else value for key, value in report_sign_word.items()}
            # 在售商品改名字典
            report_sign_word = {key: f'Active+Listings+Report+{date}' if
            'avtive_listing' in key.lower() else value for key, value in report_sign_word.items()}

            # 全部商品改名字典
            report_sign_word = {key: f'All+Listings+Report+{date}' if
            'all_listing' in key.lower() else value for key, value in report_sign_word.items()}
            # 订单报告改名字典
            report_sign_word = {key: f'All Orders-{date}' if
            'order' in key.lower() else value for key, value in report_sign_word.items()}

            for file in all_report_files:
                for key in report_sign_word.keys():
                    if key in file.lower():
                        new_file_dirname = report_sign_word[key]
                        file_type = os.path.splitext(file)[-1]
                        try:
                            os.rename(os.path.join(files_folder, file),
                                      os.path.join(files_folder, new_file_dirname + file_type))
                            break
                        except:
                            break

        def download_from_api(api_dir: 'dir', files_save_dirname, station_name):
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
                out_content = request_file.content
                file_dirname = os.path.join(files_save_dirname, station_name)
                if not os.path.exists(file_dirname):
                    try:
                        os.makedirs(file_dirname)
                    except Exception as e:
                        print(e)
                        print(f'{file_dirname}:文件存在.')
                files_save_dirname = os.path.join(file_dirname, file_basename)
                # print(file_dirname)
                with open(files_save_dirname, 'wb') as f:
                    f.write(out_content)
            else:
                if 'MonthToDate' in newest_dir:
                    return
                print(f'无法请求{newest_dir}报表! \n status_code:{status_code}')

        # 1.得到广告报表
        if set(['ST', 'BR', 'AO', 'AC', 'AL', 'CP']) != all_file_key:
            lost_file = set(['ST', 'BR', 'AO', 'AC', 'AL', 'CP']) - set(all_file_key)
            print(f'{station_name}缺失 {lost_file}报表.')
        else:
            for key in all_file_dict.keys():
                for i in range(len(all_file_dict[key])):
                    download_from_api(all_file_dict[key][i], files_save_dirname, station_name)
        # station_folder_full_dir = os.path.join(files_save_dirname, station_name)
        unified_reports_name(station_save_folder, station_name)
        request_file_complete_detect(station_save_folder, station_name)
        # zip_station_folder(station_save_folder, station_name)


def request_file_result(all_stations_name, request_save_folder=r"D:\api_request_all_files"):
    now_date = str(datetime.now().date())
    request_log_save_path = request_save_folder
    request_save_folder = os.path.join(request_save_folder, now_date)
    if not os.path.exists(request_save_folder):
        return
    requests_stations_name = os.listdir(request_save_folder)
    all_file_result = []
    for station_name in requests_stations_name:
        station_folder = os.path.join(request_save_folder, station_name)
        station_request_result = request_log(station_folder)
        all_file_result.append(station_request_result)
    # 无法post的站点
    stations_not_post_name = set(all_stations_name) - set(requests_stations_name)
    if stations_not_post_name:
        [all_file_result.append(
            [station_name, '无法post', 10, '不存在', '不存在', '不存在', '不存在', '不存在', '不存在', '不存在', '不存在', '不存在',
             '不存在']) for station_name in stations_not_post_name]

    columns_name = ['站点名', '完整情况', '报表缺失数量', 'camp_30', 'order', 'br', 'st', 'al', 'ac', 'camp_1', 'camp_7', 'camp_14',
                    'camp_60']
    now_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    request_result = pd.DataFrame(all_file_result, columns=columns_name)
    summary_columns = ['时间', '站点数', '完整站点数', '不完整站点数', '无法post数', 'camp_30', 'order', 'br', 'st', 'al', 'ac',
                       'camp_1',
                       'camp_7', 'camp_14', 'camp_60']
    summary_data = [now_date, len(all_stations_name),
                    len(request_result[(request_result['camp_30'] == '存在') & (request_result['br'] == '存在')
                                       & (request_result['st'] == '存在') & (request_result['camp_1'] == '存在') & (
                                               request_result['camp_7'] == '存在')
                                       & (request_result['camp_14'] == '存在') & (request_result['camp_60'] == '存在')]),
                    len(request_result[(request_result['camp_30'] == '不存在') | (request_result['br'] == '不存在')
                                       | (request_result['st'] == '不存在') | (request_result['camp_1'] == '不存在') |
                                       (request_result['camp_7'] == '不存在') | (request_result['camp_14'] == '不存在') |
                                       (request_result['camp_60'] == '不存在')]),
                    len(request_result[request_result['完整情况'] == '无法post']),
                    len(request_result[request_result['camp_30'] == '存在']),
                    len(request_result[request_result['order'] == '存在']),
                    len(request_result[request_result['br'] == '存在']),
                    len(request_result[request_result['st'] == '存在']),
                    len(request_result[request_result['al'] == '存在']),
                    len(request_result[request_result['ac'] == '存在']),
                    len(request_result[request_result['camp_1'] == '存在']),
                    len(request_result[request_result['camp_7'] == '存在']),
                    len(request_result[request_result['camp_14'] == '存在']),
                    len(request_result[request_result['camp_60'] == '存在'])]
    summary_info = pd.DataFrame([summary_data], columns=summary_columns)
    writer = pd.ExcelWriter(os.path.join(request_log_save_path, f'request_log_{now_date}.xlsx'))
    summary_info.to_excel(writer, startrow=2, index=False, sheet_name='站点报表请求情况')
    request_result.to_excel(writer, startrow=6, index=False, sheet_name='站点报表请求情况')
    writer.save()


def thread_read_file():
    all_task = []
    for one_page in range(4):
        all_task.append(THREAD_POOL.submit(request_save_all_6_files))
    for future in as_completed(all_task):
        future.result()


def process_read_file():
    # now_date = str(datetime.now().date())
    # 删除以前的站点数据，释放空间
    service_save_folder_path = r"F:\api_request_all_files"
    file_list = os.listdir(service_save_folder_path)
    today_date_str = datetime.now().strftime('%Y-%m-%d')
    [shutil.rmtree(os.path.join(service_save_folder_path, file)) for file in file_list if ('.' not in file) & (file != today_date_str)]

    for key in red.keys():
        red.delete(key)
    for key in red_station_status.keys():
        red_station_status.delete(key)
    # 请求的总站点数
    stations_name_n_manger = db_download_station_names()
    stations_name = stations_name_n_manger['station_name']

    def get_other_company_station(path):
        station_list = pd.read_excel(path)['广告后台店铺名']
        station_list = [station.replace('-','_').replace(' ','_').lower() for station in station_list]
        return station_list
    # 非易佰站点名
    path = r"E:\ad_zyp\api_request_files\非易佰站点.xlsx"
    other_company_station = get_other_company_station(path)

    # 冻结的站点
    fz_stations = pd.read_excel(r"E:\ad_zyp\api_request_files\冻结站点.xlsx")
    fz_stations= fz_stations['冻结站点'].apply(lambda x:x.lower())

    #请求的站点数
    stations_name=set(stations_name) - set(other_company_station)
    stations_name = set(stations_name - set(fz_stations))
    red.rpush('requests_stations_name', *stations_name)
    red.rpush('requests_stations_name_backup', *stations_name)
    stations_num = red.llen('requests_stations_name')
    print(f"此次请求{stations_num}个站点.")

    while 1:
        all_task = []
        for one_page in range(2):
            all_task.append(PROCESS_POOL.submit(thread_read_file))
        for future in as_completed(all_task):
            future.result()
        if red.llen('requests_stations_name') == 0:
            print(f"{stations_num}个站点全部完成。")
            # request_file_result(stations_name)
            gather_requests_result()
            break


if __name__ == '__main__':
    while 1:
        hour = datetime.now().hour
        if hour ==  13:
            start_time = datetime.now()
            process_read_file()
            finish_time = datetime.now()
            print(f"一共花费{finish_time - start_time}")
            if datetime.now().hour != 0:
                time.sleep(1800)
        else:
            print('休息半小时')
            time.sleep(1800)
