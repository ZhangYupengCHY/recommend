import requests
import json
import string
import pandas as pd
import my_toolkit.public_function as public_function

if __name__ == '__main__':

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
