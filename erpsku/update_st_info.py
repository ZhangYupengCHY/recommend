"""
更新st_info数据
将路径E:\AD_WEB\file_dir\st_info下全部站点的st报表文件中的全部工作表数据存储在
mysql的server_camp_report数据库中
    工作表：                                        数据库表
    出单优质搜索词
    未出单高点击搜索词
    近期低于平均点击率的SKU
    后台Search Term参考
    不出单关键词
    同erp_sku下其他seller_sku出单关键词-全部        erpsku_restkws_add_columns
    同erp_sku下其他seller_sku出单关键词-同国家      erpsku_restkws_add_columns_filter_langs
"""
import os
import time
import string
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import warnings
import copy
import gc
from sqlalchemy.types import VARCHAR, Float, Integer, Date, Numeric

import my_toolkit.process_files as process_files
import my_toolkit.public_function as public_function
import my_toolkit.conn_db as conn_db

warnings.filterwarnings('ignore')


# 从公司的服务器加载erpsku信息到redis中
def trans_erpsku_info_from_sql_2_redis():
    """
    从公司的服务器中加载erpsku信息,
    进行处理得到erpsku,asin,sellersku后,
    将信息存储到redis中其中
    key 以 erpsku_info_时间(日期_小时) db=0
    :return:None
    """
    # 加载erpsku信息
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'mrp_read', 'mrpread', '47.106.127.183', 3306, 'mrp_py', 'utf8'))
    conn = engine.connect()
    select_erpsku_sql = 'SELECT 标识,erp_sku FROM gross_require'
    erpsku_info = pd.read_sql(select_erpsku_sql, conn)
    conn.close()
    # 将数据库信息处理后生成erpsku,asin,sellersku
    """
    标识：
    'Socialme美国$SMU - JHX - 8
    WMb0P - JY18828 - 02
    FBA$X0025AUPJP - B07Q6W6LXC @ JY18828 - 02'
    由station_name$sellersku$fnsku-asin@erpsku 
    """
    erpsku_info['seller_sku'] = erpsku_info['标识'].apply(lambda x: x.split('$')[1])
    erpsku_info['asin'] = erpsku_info['标识'].apply(lambda x: x.split('$')[2][11:21])
    erpsku_info.rename(columns={'erp_sku': 'erpsku'}, inplace=True)
    erpsku_info = erpsku_info[['erpsku', 'asin', 'seller_sku']]
    # 将erpsku_info存储到redis中
    now_datetime = datetime.now().strftime('%Y-%m-%d_%H')
    erpsku_redis_key = 'erpsku_info_{}'.format(now_datetime)
    conn_redis = public_function.Redis_Store(decode_responses=False, db=0)
    conn_redis.redis_upload_df(erpsku_redis_key, erpsku_info)
    conn_redis.close()


def upload_file(st_file_path, erpsku_sellersku_info):
    """
    将st表中工作表的数据添加station,erpsku,asin和datetime后上传到数据库
    :param file_path:str
            st表路径
    :param erpsku_sellersku_info:dataframe
            erpsku与sellersku对应的df
    :return: None
    """
    # 获得station_name
    station_name = os.path.basename(st_file_path).split(' ')
    if not station_name:
        return
    station_name = station_name[1][:-3] + '_' + station_name[1][-2:]
    station_name = station_name.upper()
    # 将每个工作簿添加生成时间和erpsku
    # todo 建立工作表与数据库名的字典,包含工作表字段与数据库字段，数据库删除语言和数据插入语言
    # 将sheet中的表头改名为sql中的字段名以及顺序
    sheet_dict = {
        '出单优质搜索词':
            {
                'sql_table_name': 'high_quality_kws',
                'rename_columns': ['sku', 'campaign_name', 'ad_group_name', 'match_type', 'customer_search_term',
                                   'impression', 'click', 'spend', 'sale', 'order', 'ctr', 'cpc', 'acos', 'cr',
                                   'sku_sale', 'item_name']
            },
        '未出单高点击搜索词':
            {
                'sql_table_name': 'high_click_no_order_kws',
                'rename_columns': ['sku', 'campaign_name', 'ad_group_name', 'match_type', 'customer_search_term',
                                   'impression', 'click', 'spend', 'sale', 'order', 'ctr', 'cpc', 'acos', 'cr',
                                   'sku_click']
            },
        '近期低于平均点击率的SKU':
            {
                'sql_table_name': 'lower_ctr_sku',
                'rename_columns': ['sku', 'impression', 'click', 'spend', 'order', 'sale', 'ctr', 'acos']
            },
        '后台Search Term参考':
            {
                'sql_table_name': 'st_refer',
                'rename_columns': ['sku', 'search_term', 'recent_orders']
            },
        '不出单关键词':
            {
                'sql_table_name': 'no_order_kws',
                'rename_columns': ['sku', 'no_order_kws_sentence', 'recent_clicks', 'kws_sentence', 'recent_orders']
            }
    }
    for sheet in sheet_dict.keys():
        try:
            st_sheet_data = process_files.read_file(st_file_path, sheet_name=sheet)
            if st_sheet_data is None:
                continue
            # 重命名
            columns = sheet_dict[sheet]['rename_columns']
            # 扩展最后的输出的数据库的列名
            extend_columns = ['station', 'erpsku', 'asin']
            extend_columns.extend(columns)
            extend_columns.append('updatetime')
            # '不出单关键词'有两种不同的列名情况
            st_sheet_data.columns = columns
            st_sheet_data_modify_datetime = datetime.strptime(time.ctime(os.path.getmtime(st_file_path)),
                                                              '%a %b %d %H:%M:%S %Y')
            st_sheet_data['station'] = station_name
            st_sheet_data['updatetime'] = st_sheet_data_modify_datetime
            st_sheet_data = pd.merge(st_sheet_data, erpsku_sellersku_info[['erpsku', 'asin', 'seller_sku']], how='left',
                                     left_on='sku',
                                     right_on='seller_sku')
            st_sheet_data = st_sheet_data[extend_columns]
            # 更新到数据库中 先删除掉原本站点存在的数据,后添加站点数据
            sku_search_db = 'server_camp_report'
            delete_sql = "delete from %s where station = '%s'" % (sheet_dict[sheet]['sql_table_name'], station_name)
            conn_db.to_sql_delete(delete_sql, db=sku_search_db)
            conn_db.to_sql_append(st_sheet_data, sheet_dict[sheet]['sql_table_name'], db=sku_search_db)
        except Exception as e:
            print(e)
            print(f'{station_name} {sheet}有问题')
    print(f'{station_name}: 完成.')


def db_upload_st_file(st_info_folder=r'E:\AD_WEB\file_dir\st_info'):
    """
    主函数,将st_info文件夹中的更新文件存储到数据库中
    :param st_info_folder: str
            st_info文件夹路径
    :return: None
    """
    # 1.从redis中获得erpsku信息,若redis中没有，则将信息从数据库中加载到redis中
    # erpsku信息存储在redis中的键是以 erpsku_info_日期_小时
    conn_redis = public_function.Redis_Store(decode_responses=True, db=0)
    redis_db0_keys = conn_redis.keys()
    erpsku_redis_key_sign = 'erpsku_info'
    now_datetime = datetime.now()
    now_date = now_datetime.strftime('%Y-%m-%d')
    now_hour = now_datetime.hour
    refresh = 7
    if now_hour >= refresh:
        erpsku_today_key = [key for key in redis_db0_keys if (erpsku_redis_key_sign in key) and (now_date in key) and (
                int(key.split('_')[-1]) >= refresh)]
        if not erpsku_today_key:
            # 更新redis中erpsku键
            [conn_redis.delete(key) for key in redis_db0_keys if erpsku_redis_key_sign in key]
            trans_erpsku_info_from_sql_2_redis()
    erpsku_exist_key = [key for key in redis_db0_keys if erpsku_redis_key_sign in key][0]
    conn_redis.close()
    conn_redis = public_function.Redis_Store(decode_responses=False, db=0)
    erpsku_info = conn_redis.redis_download_df(erpsku_exist_key)
    conn_redis.close()
    # 获得更新的st报表
    # 先初始化st
    old_files_list = [os.path.join(st_info_folder, file) for file in os.listdir(st_info_folder) if 'ST' in file]
    old_files_modify_time = {file: os.path.getmtime(file) for file in old_files_list}
    while 1:
        new_files_list = [os.path.join(st_info_folder, file) for file in os.listdir(st_info_folder) if 'ST' in file]
        new_files_modify_time = {file: os.path.getmtime(file) for file in new_files_list}
        process_st_files = [file for file, file_time in new_files_modify_time.items() if
                            file_time != old_files_modify_time.get(file, None)]
        if process_st_files:
            st_files = [file for file in process_st_files]
            for file in st_files:
                upload_file(file, erpsku_info)
        else:
            time.sleep(30)
            print('暂无st表更新,休息60秒.')
        old_files_modify_time = copy.deepcopy(new_files_modify_time)
        time.sleep(30)
        if datetime.now().hour in set(range(0, 7)):
            # 将erpsku搜索词数据库中的共享关键词表和同语言的共享关键词表同步到新的数据库中,并将搜索词分裂成几列
            # 来便于提高用户的搜索速度,同时应该添加数据更新时间
            erpsku_restkws_sql = "SELECT * FROM erpsku_restkws_add_columns"
            erpsku_restkws_ori_data = conn_db.read_table(erpsku_restkws_sql, db='server_camp_report')
            erpsku_restkws_same_langs_sql = "SELECT * FROM erpsku_restkws_add_columns_filter_langs"
            erpsku_restkws_same_langs_data = conn_db.read_table(erpsku_restkws_same_langs_sql, db='server_camp_report')

            def detect_list_lang(words: list):
                """
                判断文字是什么语言:
                    若语言的开头和结尾是以英文或是数字开头,这可以判断为是英语
                    若语言的开头和结尾不是以英文或是数字开头,则需要判断文字是 最接近如下
                        ['it', 'en', 'de', 'fr', 'es', 'ja', 'zh']
                Args:
                    words:list
                        需要检测的一组文字
                Returns:list
                        文字的检测结果
                """
                list_lang = ['en' if len(word) < 2 else 'en' if (word[0] in string.printable) and (
                        word[-1] in string.printable) else public_function.detect_lang(word) for word in words]
                lang_dict = {'en': 'english', 'ja': 'japanese', 'zh': 'chinese', 'it': 'italian', 'de': 'german',
                             'fr': 'french', 'es': 'spanish'}
                return [lang_dict[lang] for lang in list_lang]

            def add_columns(df):
                rest_kw_langs = detect_list_lang(list(df['rest_kw']))
                df['rest_kws_list'] = list(map(public_function.split_sentence, list(df['rest_kw']), rest_kw_langs))
                for i in range(10):
                    df[f'keyword_{i + 1}'] = [word_list[i] if len(word_list) > i else '' for word_list in
                                              df['rest_kws_list'].values]
                now_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                df['updatetime'] = now_datetime
                del df['rest_kws_list']

            def to_sql_replace(df, table, db='team_station'):
                # 执行sql
                engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
                    'user', '', 'wuhan.yibai-it.com', 33061, db, 'utf8'))
                conn = engine.connect()
                try:
                    # 将数据写入到dataframe中
                    index_columns = ['account_site', 'seller_sku', 'erp_sku', 'asin', 'keyword_1', 'keyword_2',
                                     'keyword_3',
                                     'keyword_4', 'keyword_5', 'keyword_6', 'keyword_7', 'keyword_8', 'keyword_9',
                                     'keyword_10']
                    columns_type = VARCHAR(length=255)
                    index_column_type = {column: columns_type for column in index_columns}
                    df.to_sql(table, conn, if_exists='replace', index=False, dtype=index_column_type)
                    # 建立索引
                    create_index_sql = "ALTER TABLE `%s` ADD INDEX 站点 (`%s`)," \
                                       "ADD INDEX seller_sku (`%s`),ADD INDEX erk_sku (`%s`),ADD INDEX asin (`%s`),ADD INDEX keyword_1 (`%s`)," \
                                       "ADD INDEX keyword_2 (`%s`),ADD INDEX keyword_3 (`%s`),ADD INDEX keyword_4 (`%s`),ADD INDEX keyword_5 (`%s`)," \
                                       "ADD INDEX keyword_6 (`%s`),ADD INDEX keyword_7(`%s`),ADD INDEX keyword_8 (`%s`),ADD INDEX keyword_9 (`%s`)," \
                                       "ADD INDEX keyword_10 (`%s`);" % (table,
                                                                         index_columns[0], index_columns[1],
                                                                         index_columns[2],
                                                                         index_columns[3],
                                                                         index_columns[4], index_columns[5],
                                                                         index_columns[6],
                                                                         index_columns[7],
                                                                         index_columns[8], index_columns[9],
                                                                         index_columns[10],
                                                                         index_columns[11],
                                                                         index_columns[12], index_columns[13])
                    engine.execute(create_index_sql)

                except Exception as e:
                    print(e)
                finally:
                    conn.close()
                    engine.dispose()

            add_columns(erpsku_restkws_ori_data)
            add_columns(erpsku_restkws_same_langs_data)

            erpsku_restkws_for_search_table = "erpsku_restkws_add_columns_for_search"
            to_sql_replace(erpsku_restkws_ori_data, erpsku_restkws_for_search_table,
                           db='server_camp_report')
            del erpsku_restkws_ori_data
            gc.collect()
            erpsku_restkws_same_langs_for_search_table = "erpsku_restkws_add_columns_filter_langs_for_search"
            to_sql_replace(erpsku_restkws_same_langs_data,
                           erpsku_restkws_same_langs_for_search_table,
                           db='server_camp_report')
            del erpsku_restkws_same_langs_data
            gc.collect()

            restart_hour = 9
            reset_time = (restart_hour - datetime.now().hour) * 3600
            time.sleep(reset_time)
            print(f'早上{restart_hour}再开始.')

            # 更新erpsku_info
            conn_redis = public_function.Redis_Store(decode_responses=True, db=0)
            redis_db0_keys = conn_redis.keys()
            erpsku_today_key = [key for key in redis_db0_keys if
                                (erpsku_redis_key_sign in key) and (now_date in key)]
            if not erpsku_today_key:
                # 更新redis中erpsku键
                [conn_redis.delete(key) for key in redis_db0_keys if erpsku_redis_key_sign in key]
                trans_erpsku_info_from_sql_2_redis()
                erpsku_exist_key = [key for key in redis_db0_keys if erpsku_redis_key_sign in key][0]
                conn_redis.close()
                conn_redis = public_function.Redis_Store(decode_responses=False, db=0)
                erpsku_info = conn_redis.redis_download_df(erpsku_exist_key)
                conn_redis.close()


if __name__ == '__main__':
    db_upload_st_file()