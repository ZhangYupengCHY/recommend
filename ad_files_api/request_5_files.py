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

import rsa, json, requests, os, redis, zipfile, shutil, re
import pandas as pd
# import Crypto.PublicKey.RSA
import base64, pymysql
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

THREAD_POOL = ThreadPoolExecutor(6)
PROCESS_POOL = ProcessPoolExecutor(2)
redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, password='', db=7, decode_responses=True)
red = redis.StrictRedis(connection_pool=redis_pool)


# 将站点数据压缩成zip
def zip_station_folder(folder_full_dir, station_name):
    if not os.path.exists(folder_full_dir):
        return
    stations_manger = db_download_station_names()
    manger = stations_manger['manger'][stations_manger['station_name'] == station_name].tolist()
    if manger:
        manger = manger[-1]
    else:
        manger = 'nobody'
        print(f'{station_name}没有广告专员.')
    zipped_file_dirname = os.path.join((os.path.dirname(folder_full_dir) + '_zipfiles'), manger)
    zip_dir = os.path.join(zipped_file_dirname, os.path.basename(folder_full_dir) + '.zip')
    if not os.path.exists(zipped_file_dirname):
        os.makedirs(zipped_file_dirname)
    f = zipfile.ZipFile(zip_dir, 'w', zipfile.ZIP_DEFLATED)
    for filename in os.listdir(folder_full_dir):
        f.write(os.path.join(folder_full_dir, filename), filename)
    f.close()


# 将站点数据压缩成zip
def zip_manger_folder(manger_fold_full_dir):
    if not os.path.exists(manger_fold_full_dir):
        return
    zipped_file_dirname = os.path.dirname(manger_fold_full_dir)
    zip_dir = os.path.join(zipped_file_dirname, os.path.basename(manger_fold_full_dir) + '.zip')
    if not os.path.exists(zipped_file_dirname):
        os.makedirs(zipped_file_dirname)
    f = zipfile.ZipFile(zip_dir, 'w', zipfile.ZIP_DEFLATED)
    for filename in os.listdir(manger_fold_full_dir):
        f.write(os.path.join(manger_fold_full_dir, filename), filename)
    f.close()
    shutil.rmtree(manger_fold_full_dir)


# 加载全部的站点名和广告专员
def db_download_station_names(db='team_station', table='only_station_info', ip='192.168.8.180', port=3306,
                              user_name='zhangyupeng', password='zhangyupeng') -> pd.DataFrame:
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
    stations_name_n_manger.drop_duplicates(inplace=True)
    conn.commit()
    cursor.close()
    conn.close()
    return stations_name_n_manger


def get_all_files_dir(station_name, key_path=r"D:\AD-Helper1\ad_helper\recommend\ad_files_api\public.key",
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
    try:
        data = json.loads(response)['data']
    except Exception as e:
        print(f"{station_name}:无法post.")
        return
    # 单独请求5种天数的广告报表
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
    response_ao = requests.post(local_url, data=post_ao_load).content
    try:
        ao_data = json.loads(response_ao)['data']
        ao_data = [data.replace('D:/phpStudy/PHPTutorial/WWW/wwwerp', 'http://192.168.9.167:8080') for data in ao_data]
    except:
        print(f"{station_name}:AO报表{post_date}本地无法请求..")
        ao_data = []
    data.extend(ao_data)

    data.extend(all_camp_post_dir)
    # print(data)
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


# 保存6种类型站点数据
def request_save_all_6_files(files_save_dirname=r'C:\KMSOFT\Download\station_report'):
    now_date = str(datetime.now().date())
    files_save_dirname = os.path.join(files_save_dirname, now_date)
    station_name = red.lpop('stations_name')
    if station_name:
        print(f"START: {station_name} ")
        all_file_dict = get_all_files_dir(station_name)
        if not all_file_dict:
            return
        all_file_key = all_file_dict.keys()
        all_file_dict = keep_newest_file_dir(all_file_dict, station_name)

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
            all_report_files = os.listdir(files_folder)
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
                                  os.path.join(files_folder, new_file_dirname+file_type))
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
                    os.makedirs(file_dirname)
                files_save_dirname = os.path.join(file_dirname, file_basename)
                # print(file_dirname)
                with open(files_save_dirname, 'wb') as f:
                    f.write(out_content)
            else:
                print(f'无法请求{newest_dir}报表! \n status_code:{status_code}')

        if set(['ST', 'BR', 'AO', 'AC', 'AL', 'CP']) != all_file_key:
            lost_file = set(['ST', 'BR', 'AO', 'AC', 'AL', 'CP']) - set(all_file_key)
            print(f'{station_name}缺失 {lost_file}报表.')
        else:
            for key in all_file_dict.keys():
                for i in range(len(all_file_dict[key])):
                    download_from_api(all_file_dict[key][i], files_save_dirname, station_name)
        station_folder_full_dir = os.path.join(files_save_dirname, station_name)
        unified_reports_name(station_folder_full_dir, station_name)
        zip_station_folder(station_folder_full_dir, station_name)


def request_file_result(all_stations_name, request_save_folder=r'C:\KMSOFT\Download\station_report'):
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
    summary_columns = ['请求时间', '请求站点数', '完整报表站点数', '不完整报表站点数', '无法post站点数', 'camp_30', 'order', 'br', 'st', 'al', 'ac',
                       'camp_1',
                       'camp_7', 'camp_14', 'camp_60']
    summary_data = [now_date, len(all_stations_name), len(request_result[(request_result['camp_30'] == '存在') & (request_result['br'] == '存在')
                                                                         & (request_result['st'] == '存在') & (request_result['camp_1'] == '存在') & (request_result['camp_7'] == '存在')
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
    now_date = str(datetime.now().date())
    save_folder_path = r'C:\KMSOFT\Download\station_report'
    files_save_dirname = os.path.join(save_folder_path, now_date + '_zipfiles')
    if os.path.exists(files_save_dirname):
        shutil.rmtree(files_save_dirname)
    red.delete('stations_name')
    stations_name_n_manger = db_download_station_names()
    stations_name = stations_name_n_manger['station_name'][119:128]
    red.rpush('stations_name', *stations_name)
    stations_num = red.llen('stations_name')
    print(f"此次请求{stations_num}个站点.")
    while 1:
        all_task = []
        for one_page in range(2):
            all_task.append(PROCESS_POOL.submit(thread_read_file))
        for future in as_completed(all_task):
            future.result()
        if red.llen('stations_name') == 0:
            print(f"{stations_num}个站点全部完成。")
            request_file_result(stations_name, request_save_folder=save_folder_path)
            now_date = str(datetime.now().date())
            stations_file_dir_folder = os.path.join(save_folder_path, now_date)
            shutil.rmtree(stations_file_dir_folder)
            # 将全部的站点按照每人一个压缩包压缩
            stations_zipped_file_dir_folder = stations_file_dir_folder + '_zipfiles'
            mangers_list = os.listdir(stations_zipped_file_dir_folder)
            if mangers_list:
                [zip_manger_folder(os.path.join(stations_zipped_file_dir_folder, manger)) for manger in mangers_list]
            print(f"FINISH:完成{len(mangers_list)}个人的站点请求!!!")
            break


if __name__ == '__main__':
    process_read_file()
