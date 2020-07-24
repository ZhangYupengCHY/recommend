import requests
import re
import json
import os
import glob
import multiprocessing
import pandas as pd
import redis
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import datetime as dt
import matplotlib.dates as mdate


# 连接redis数据库
def download_redis():
    r = redis.Redis(host='localhost', port=6379, password='chy910624', decode_responses=True)
    # 遍历redis数据库
    keys = r.keys()
    values = []
    for key in keys:
        value = r.get(key)
        values.append(value)
    return [keys, values]


def trans_redis_2_df(redis_keys_n_values):
    column_name = ['account', 'site', 'dates', 'warning_msg']
    df = pd.DataFrame(columns=column_name)
    for key, value in zip(redis_keys_n_values[0], redis_keys_n_values[1]):
        row = key.split(':')[2:8:2]
        row.append(value)
        df.loc[df.shape[0] + 1] = row
    return df


def request_cam_download_api(base_dir, **kwargs):
    global now_station_dir
    cam_url = "http://120.78.243.154//services/advertising/reportdownload/campaign"
    post_load = {
        'site': kwargs['site'].lower().replace('es', 'sp'),
        'account': kwargs['account'].capitalize(),
        'report_start_date': kwargs['report_start_date'],
        'report_end_date': kwargs['report_end_date'],
    }
    # print self.cam_url
    # print post_load
    response = requests.post(cam_url, data=post_load).content
    response = response.decode()
    if not re.search('"code":1000', response):
        return response
    status, msg, result = (json.loads(response)).values()
    # print(result)

    # 创造输出路径
    # base_dir = 'C:/KMSOFT/Download/bulk_reports'
    folder_name = kwargs['account'] + "_" + kwargs['site']
    if not os.path.exists(base_dir):
        os.mkdir(base_dir)
    now_station_dir = base_dir + "/" + folder_name
    if not os.path.exists(now_station_dir):
        os.mkdir(now_station_dir)

    # 获取列表中的文件流
    if result:
        for one_file_dict in result:
            file_date = one_file_dict['report_date']
            file_url = one_file_dict['file']
            # print(file_url)
            file_name = file_url.split('/')[-1]
            file_r = requests.get(file_url)
            status_code = file_r.status_code
            if status_code == 200:
                with open(now_station_dir + '/' + file_name, 'wb') as f:
                    f.write(file_r.content)
                # return "ok"
            # else:
            #     return status_code
    # else:
    #     return status


# 将数据存在redis:
def upload_to_redis(series):
    r = redis.Redis(host='localhost', port=6379, password='chy910624', decode_responses=True)
    r.set('account:{}:site:{}:date:{}'.format(series[0], series[1], series[2]), \
          'acos:{}:cpc:{}:cr:{}:spend:{}:sales:{}'.format(series[3], series[4], series[5], series[6], series[7]))


# 连接redis数据库
def download_redis():
    r = redis.Redis(host='localhost', port=6379, password='chy910624', decode_responses=True)
    # 遍历redis数据库
    keys = r.keys()
    values = []
    for key in keys:
        value = r.get(key)
        values.append(value)
    list1 = key[0].split(":")[1:7:2]
    list1.extend(value[0].split(":")[1:11:2])
    series = pd.DataFrame(list1, columns=['account', 'site', 'date', 'acos', 'cpc', 'cr', 'spend', 'sales'])
    return series


def add_list(list_a):
    list_a = [round(i) if not isinstance(i, int) else i for i in list_a]
    s = []
    for i in range(len(list_a)):
        s.append(sum(list_a[:i + 1]))
    return s


# 计算每天的广告数据，曝光、点击、花费等
def every_day_campaign(one_dir):
    file_date = parse_date(one_dir)
    file_data = pd.read_excel(one_dir, sheet_name=1)
    file_data[['Clicks', 'Orders', 'Impressions']] = file_data[['Clicks', 'Orders', 'Impressions']].applymap(
        lambda x: int(x))
    file_data[['Spend', 'Sales']] = file_data[['Spend', 'Sales']].applymap(lambda x: float(x))
    one_day_clicks = sum(file_data['Clicks'])
    one_day_impression = sum(file_data['Impressions'])
    one_day_orders = sum(file_data['Orders'])
    one_day_spend = sum(file_data['Spend'])
    one_day_sales = sum(file_data['Sales'])
    if one_day_sales == 0:
        acos = 0
    else:
        acos = one_day_spend / one_day_sales
    if one_day_clicks == 0:
        cpc, cr = 0
    else:
        cpc = one_day_spend / one_day_clicks
        cr = one_day_orders / one_day_clicks
    account = 'Nitrip'
    site = 'uk'
    one_day_info = [account, site, file_date, acos, cpc, cr, one_day_spend, one_day_sales, one_day_impression,
                    one_day_clicks]
    return one_day_info
    # # 数据入到redis
    # pass
    # upload_to_redis(all_info)


# 获得日期字符串
def parse_date(file_name):
    file_date = re.findall('2019.[0-9]{2}.[0-9]{2}', file_name)
    file_date = file_date[-1]
    if len(file_date) == 10:
        file_date = file_date[0:4] + '-' + file_date[5:7] + '-' + file_date[-2:]
    if len(file_date) == 8:
        file_date = file_date[0:4] + '-' + file_date[4:6] + '-' + file_date[-2:]
    return file_date


# 计算每天的广告数据
def get_imp_cpc(file_dir, account_site):
    all_file_dir = glob.glob(file_dir + '/' + account_site.lower() + '/*xlsx')
    pool = multiprocessing.Pool(3)
    all_result = pd.DataFrame(
        columns=['account', 'site', 'date', 'acos', 'cpc', 'cr', 'spend', 'sales', 'impression', 'clicks'])
    results = []
    for one_dir in all_file_dir:
            results.append(pool.apply_async(every_day_campaign, (one_dir,)))
        # result = every_day_campaign(one_dir)
        # all_result.loc[all_result.shape[0] + 1] = result
    pool.close()
    pool.join()
    for result in results:
        one_info = result.get()
        all_result.loc[all_result.shape[0] + 1] = one_info
    # =============================================================================
    for i in ['spend', 'sales', 'impression', 'clicks']:
        all_result['add_{}'.format(i)] = add_list(list(all_result[i]))
    return all_result


# 获取redis中的数据进行展示
def show_info(df):
    bar_width = 0.25
    cc = df
    # 实例化
    fig = plt.figure()
    x = cc['date'].values.astype(str)
    x = [datetime.strptime(i, "%Y-%m-%d") for i in x]
    x1 = [i + dt.timedelta(hours=6) for i in x]
    x2 = [i + dt.timedelta(hours=12) for i in x]
    x3 = [i + dt.timedelta(hours=18) for i in x]
    # 创造子图
    ax1 = fig.add_subplot(211)
    # 绘图
    ax1.plot(x, cc['acos'], color='black', label='acos')
    # 添加图例acos
    for a, b in zip(x, cc['acos']):
        if b != 0:
            plt.text(a, b + 0.005, '{:0.2f}%'.format(b * 100), ha='center', va='bottom', fontsize=7)

    ax1.bar(x, cc['cpc'], color='red', label='cpc', width=bar_width)
    # 添加图例cpc
    for a, b in zip(x, cc['cpc']):
        if b != 0:
            plt.text(a, b + 0.005, '{:0.2f}%'.format(b * 100), ha='center', va='bottom', fontsize=7)
            # 添加图例cr
    for a, b in zip(x1, cc['cr']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.2f}%'.format(b * 100), ha='center', va='bottom', fontsize=7)
    ax1.bar(x1, cc['cr'], color='blue', label='cr', width=bar_width)
    # 设置横纵标签
    ax1.set_ylabel('acos/cpc/cr')
    ax1.set_title("Nitrip_uk")
    # 图例
    plt.legend(loc="upper left")
    ax1.set_xlabel('date')
    # 设置X轴的刻度
    plt.xticks(pd.date_range(min(x), max(x)))  # 设置时间标签显示格式
    ax1.xaxis.set_major_formatter(mdate.DateFormatter('%m-%d'))  # 设置时间标签显示格式

    ax2 = fig.add_subplot(212)

    # 设置横纵标签
    ax2.set_ylabel('spend/sales/impression/clicks')
    ax2.bar(x, cc['spend'], color='yellow', width=bar_width, label='spend')
    ax2.bar(x1, cc['sales'], color='green', width=bar_width, label='sales')
    ax2.bar(x2, cc['impression'] / 100, color='blue', width=bar_width, label='impression')
    ax2.bar(x3, cc['clicks'], color='black', width=bar_width, label='clicks')
    # 图例
    plt.legend(loc="upper left")
    ax2.set_xlabel('date')
    # 设置X轴的刻度
    plt.xticks(pd.date_range(min(x), max(x)))  # 设置时间标签显示格式
    ax2.xaxis.set_major_formatter(mdate.DateFormatter('%m-%d'))  # 设置时间标签显示格式
    # 添加图例spend
    for a, b in zip(x, cc['spend']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)
    for a, b in zip(x1, cc['sales']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)
    for a, b in zip(x2, cc['impression'] / 100):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)
    for a, b in zip(x3, cc['clicks']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)

    # 副坐标
    ax3 = ax2.twinx()  # this is the important function
    ax3.set_ylabel("ADD spend/sales/impression/clicks")
    ax3.plot(x, cc['add_spend'], color='yellow', label='add_spend')
    ax3.plot(x, cc['add_sales'], color='green', label='add_sales')
    ax3.plot(x, cc['add_impression'] / 100, color='blue', label='add_impression')
    ax3.plot(x, cc['add_clicks'], color='black', label='add_clicks')
    plt.legend(loc="upper right")
    plt.show()
    pass


if __name__ == "__main__":
    global account, site
    account = 'Nitrip'
    site = 'uk'
    # =============================================================================
    #     request_args_test = {'site': '{}'.format(site),
    #                          'account': '{}'.format(account),
    #                          'report_start_date': '2019-8-20 00:00:00',
    #                          'report_end_date': '2019-9-9 00:00:00',
    #                          }
    base_dir_test = "C:/Users/Administrator/Desktop"
    #     request_cam_download_api(base_dir_test, **request_args_test)
    #     mm = get_imp_cpc(base_dir_test,'Nitrip_uk')
    # =============================================================================
    df = get_imp_cpc(base_dir_test, 'Nitrip_uk')
    df.sort_values(by='date', inplace=True)
    #  show_info(df)



















