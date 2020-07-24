# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/4/24 11:52
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""
import os
from datetime import datetime

import pandas as pd
import pymysql
import re
from my_toolkit import process_files

acos_ideal = {'CA': 0.14, 'DE': 0.15, 'FR': 0.15, 'IT': 0.15, 'SP': 0.15, 'JP': 0.15,
              'UK': 0.18, 'MX': 0.15, 'IN': 0.18, 'US': 0.18, 'ES': 0.15, 'AU': 0.15}

# cpc最高出价
cpc_max = {'CA': 0.4, 'DE': 0.35, 'FR': 0.35, 'IT': 0.3, 'SP': 0.3, 'JP': 25,
           'UK': 0.4, 'MX': 2.5, 'IN': 4.5, 'US': 0.5, 'ES': 0.3, 'AU': 0.4}

# 站点的最小出价
ad_group_max_bid_lower_limit_dict = {'US': 0.02, 'CA': 0.02, 'MX': 0.1, 'UK': 0.02, 'DE': 0.02, 'FR': 0.02, 'IT': 0.02,
                                     'ES': 0.02, 'JP': 2, 'AU': 0.02, 'IN': 1, 'AE': 0.24}



# 连接数据库得到站点平均cpc,acos,返回station,acos,cpc,站点负责人四列
def db_download_station_names(db='team_station', table='only_station_info', ip='wuhan.yibai-it.com', port=33061,
                              user_name='marmot', password=''):
    """
    加载广告组接手的站点以及对应的平均数据
    :return: 站点平均cpc和acos
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
    sql = """SELECT station,acos,cpc,ad_manger FROM {} """.format(table)
    # 执行sql语句
    cursor.execute(sql)
    stations_avg = cursor.fetchall()
    stations_avg = pd.DataFrame([list(station) for station in stations_avg],
                                columns=['station', 'acos', 'cpc', 'ad_manger'])
    stations_avg.drop_duplicates(inplace=True)
    conn.commit()
    cursor.close()
    conn.close()
    return stations_avg


# processed_files 1.处理精否/否定asin
def negative_exact(station_name, camp_data, st_data, active_listing_data):
    """
    描述:
        用户搜索的内容中,点击或是曝光产生不了收益，这些词或是ASIN我们可以认为是无效的,于是可以将
        这部分用户搜索词或是ASIN停掉,不再产生花费。
    逻辑:
        否定分为:处理精否(关键词keyword)和否定ASIN(ASIN)
        否定关键词主逻辑：
            # 条件1： click>40,order=0;
            # 条件2： spend>站点平均cpc*40,order=0;
            # 条件3： acos>站点平均acos*3,cr<2%;
            # 条件4：acos>站点平均acos*3,cr<3%,click>40*(order+1);
            # 条件5: acos>站点平均acos*3,spend>站点平均cpc*40*(order+1)
        否定ASIN主逻辑:
            # 通过click去否定
    :param station_name:站点名
    :param camp_data:广告报表
    :param st_data:搜索词报表
    :param active_listing_data:用于sku和asin之间的关系
    :param stations_folder:站点文件夹存储的路径
    :return:
    """

    # 按照条件处理需要精否的keywords,生成精否过程表
    def get_negative_exact_kws(station_name, st_data, camp_data):
        if st_data is None:
            return
        if st_data.empty:
            return
        # 保留关键词,剔除掉B0,b0开头(ASIN否定)
        site = station_name[-2:].upper()
        stations_info = db_download_station_names()
        station_acos = stations_info['acos'][stations_info['station'] == station_name.lower()]
        if station_acos.empty:
            station_acos = None
            print(f'站点不存在:站点{station_name}无法在only_station_info中找到.')
            return
        else:
            station_acos = station_acos.values[0]
            station_acos = float(station_acos.replace('%', '')) / 100
            # 防止站点acos过高，设置一个上限
            station_acos = min(station_acos, acos_ideal[site] * 2)

        station_cpc = stations_info['cpc'][stations_info['station'] == station_name.lower()]
        if station_cpc.empty:
            station_cpc = None
            print(f'站点不存在:站点{station_name}无法在only_station_info中找到.')
            return
        else:
            station_cpc = station_cpc.values[0]
            # 防止站点cpc过高，设置一个上限
            station_cpc = min(station_cpc, cpc_max[site])

        # 筛选出需要的列
        # clicks,spend,7 day total orders,advertising cost of sales(acos)
        need_columns = ['Campaign Name', 'Ad Group Name', 'Customer Search Term', 'Match Type',
                        'Impressions', 'Clicks', 'Spend', '7 Day Total Orders (#)', '7 Day Total Sales',
                        'Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate']
        if not set(need_columns).issubset(set(st_data.columns)):
            print(f'缺失列: 站点 {station_name} st表缺失{set(need_columns) - set(st_data.columns)}列。')
            return
        # 将acos和cr转化成数值型
        st_data[['Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate']] = st_data[
            ['Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate']].applymap(
            lambda x: x if not isinstance(x, str) else float(x) if '%' not in x else float(x.replace('%', '')) / 100)

        # st表中的关键词搜索
        st_keywords_data = st_data[~st_data['Customer Search Term'].str.startswith(('B0', 'b0'))]
        # 查找满足条件的关键词

        # 条件1： click>40,order=0
        negative_exact_kws_1 = st_keywords_data[need_columns][
            (st_keywords_data['Clicks'] > 40) & (st_keywords_data['7 Day Total Orders (#)'] == 0)]
        # 条件2： spend>站点平均cpc*40,order=0
        if station_cpc is not None:
            negative_exact_kws_2 = st_keywords_data[need_columns][
                (st_keywords_data['Spend'] > station_cpc * 40) & (st_keywords_data['7 Day Total Orders (#)'] == 0)]
        else:
            negative_exact_kws_2 = None
        # 条件3： acos>站点平均acos*3,cr<2%;
        if station_acos is not None:
            negative_exact_kws_3 = st_keywords_data[need_columns][
                (st_keywords_data['Advertising Cost of Sales (ACoS)'] > station_acos * 3) & (
                        st_keywords_data['7 Day Conversion Rate'] < 0.02)]
        else:
            negative_exact_kws_3 = None
        # 条件4：acos>站点平均acos*3,cr<3%,click>40*(order+1);
        if station_acos is not None:
            negative_exact_kws_4 = st_keywords_data[need_columns][
                (st_keywords_data['Advertising Cost of Sales (ACoS)'] > station_acos * 3) & (
                        st_keywords_data['7 Day Conversion Rate'] < 0.03) &
                (st_keywords_data['Clicks'] > (st_keywords_data['7 Day Total Orders (#)'] + 1) * 40)]
        else:
            negative_exact_kws_4 = None
        # 条件5: acos>站点平均acos*3,spend>站点平均cpc*40*(order+1)
        if station_acos is not None:
            negative_exact_kws_5 = st_keywords_data[need_columns][
                (st_keywords_data['Advertising Cost of Sales (ACoS)'] > station_acos * 3) &
                (st_keywords_data['Spend'] > (st_keywords_data['7 Day Total Orders (#)'] + 1) * 40 * station_cpc)]
        else:
            negative_exact_kws_5 = None
        all_negative_exact_kws = [kws for kws in [negative_exact_kws_1, negative_exact_kws_2, negative_exact_kws_3,
                                                  negative_exact_kws_4, negative_exact_kws_5] if kws is not None]
        if all_negative_exact_kws:
            all_negative_exact_kws = pd.concat(all_negative_exact_kws)

        # st表中的ASIN搜索
        st_asin_data = st_data[st_data['Customer Search Term'].str.startswith(('B0', 'b0'))]
        # 查找满足条件的asin
        negative_exact_asin = st_asin_data[need_columns][
            (st_asin_data['Clicks'] > 40) & (st_asin_data['7 Day Total Orders (#)'] == 0)]

        # 将asin否定关键词和keyword否定关键词合并处理
        all_negative_exact = pd.concat([all_negative_exact_kws, negative_exact_asin])

        all_negative_exact.drop_duplicates(inplace=True)
        # 输出精否临时表
        now_date = datetime.now().date()
        # 删除camp表中重复的精否词
        if (camp_data is not None) & (not camp_data.empty):
            # 处理camp表的列名
            camp_data.columns = [column.strip(" ") for column in camp_data.columns]
            camp_negative_exact_data = camp_data[
                (camp_data['Match Type'].str.contains('negative', case=False)) & (
                        camp_data['Campaign Status'] == 'enabled') &
                (camp_data['Ad Group Status'] == 'enabled') & (camp_data['Status'] == 'enabled')]
            if not camp_negative_exact_data.empty:
                for index, row in all_negative_exact.iterrows():
                    st_camp_name = row['Campaign Name']
                    st_group_name = row['Ad Group Name']
                    st_kws = row['Customer Search Term']
                    camp_negative_exact_haved = camp_negative_exact_data[
                        (camp_negative_exact_data['Campaign'] == st_camp_name)
                        & (camp_negative_exact_data['Ad Group'] == st_group_name)
                        & (camp_negative_exact_data['Keyword or Product Targeting'] == st_kws)]
                    if not camp_negative_exact_haved.empty:
                        all_negative_exact.drop(index=index, inplace=True)
        all_negative_exact.reset_index(drop=True, inplace=True)
        # 通过camp表 添加Max bid和Status两列
        for index in all_negative_exact.index:
            max_bid = camp_data['Max Bid'][
                (camp_data['Campaign'] == all_negative_exact.loc[index, 'Campaign Name']) &
                (camp_data['Ad Group'] == all_negative_exact.loc[index, 'Ad Group Name']) &
                (camp_data['Keyword or Product Targeting'] == all_negative_exact.loc[
                    index, 'Customer Search Term'])]
            if not max_bid.empty:
                all_negative_exact.loc[index, 'Max bid'] = max_bid.values[0]
            else:
                all_negative_exact.loc[index, 'Max bid'] = None

            status = camp_data['Status'][(camp_data['Campaign'] == all_negative_exact.loc[index, 'Campaign Name']) &
                                         (camp_data['Ad Group'] == all_negative_exact.loc[index, 'Ad Group Name']) &
                                         (camp_data['Keyword or Product Targeting'] == all_negative_exact.loc[
                                             index, 'Customer Search Term'])]
            if not status.empty:
                all_negative_exact.loc[index, 'Status'] = status.values[0]
            else:
                camp_da_group_status = camp_data['Ad Group Status'][
                    (camp_data['Campaign'] == all_negative_exact.loc[index, 'Campaign Name']) &
                    (camp_data['Ad Group'] == all_negative_exact.loc[index, 'Ad Group Name'])]
                if not camp_da_group_status.empty:
                    all_negative_exact.loc[index, 'Status'] = camp_da_group_status.values[0]

                else:
                    all_negative_exact.loc[index, 'Status'] = None

            export_columns = ['Campaign Name', 'Ad Group Name', 'Customer Search Term', 'Max bid', 'Match Type',
                              'Impressions', 'Clicks', 'Spend', '7 Day Total Orders (#)', '7 Day Total Sales',
                              'Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate', 'Status']
            all_negative_exact = all_negative_exact[export_columns]
            all_negative_exact.drop_duplicates(inplace=True)

        # 输出精否逻辑
        def negative_exact_logic(avg_cpc, avg_acos):
            '''
            logic1:click>40,order=0
            logic2:spend>站点平均cpc*40,order=0
            logic3:acos>站点平均acos*3,cr<2%
            logic4:acos>站点平均acos*3,cr<3%,click>40*(order+1)
            logic5:acos>站点平均acos*3,spend>站点平均cpc*(order+1)
            '''
            negative_exact_logic = pd.DataFrame([['click>40 & order=0', f'spend>{round(avg_cpc * 40, 2)} & order=0',
                                                  f'acos>{round(avg_acos * 3, 2)} & cr<2%',
                                                  f'acos>{round(avg_acos * 3, 2)} & cr<3% & click>40*(order+1)',
                                                  f'acos>{round(avg_acos * 3, 2)} & sp\
                                                                        end>{round(avg_cpc, 2)}*(order+1)']],
                                                columns=['logic1', 'logic2', 'logic3', 'logic4', 'logic5'])
            return negative_exact_logic
            # negative_exact_logic.to_excel(file_save_path, index=False, sheet_name='精否条件')

        # 输出精否过程表和精否条件
        def export_negative_exact_logic_n_kws(all_negative_exact_kws, negative_exact_logic, file_save_path):
            if (all_negative_exact_kws.empty) or (all_negative_exact_kws is None):
                return
            write = pd.ExcelWriter(file_save_path)
            all_negative_exact_kws.to_excel(write, sheet_name='精否过程表', index=False)
            negative_exact_logic.to_excel(write, sheet_name='精否条件', index=False)
            write.save()

        # 输出精否过程表中精否条件
        # negative_exact_logic_info = negative_exact_logic(station_cpc, station_acos)
        # export_negative_exact_logic_n_kws(all_negative_exact, negative_exact_logic_info, file_save_path)
        return all_negative_exact

    # 生成精否表
    def negative_kws_file(station_name, negative_exact_kws_data, camp_data, active_listing_data):
        if negative_exact_kws_data is None:
            return
        if negative_exact_kws_data.empty:
            return
        std_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                       'Campaign Targeting Type', 'Ad Group Name',
                       'Max Bid', 'SKU', 'Keyword', 'Product Targeting ID', 'Match Type', 'Campaign Status',
                       'Ad Group Status', 'Status', 'Bidding strategy']
        negative_exact_kws_upload_format = pd.DataFrame(columns=std_columns)
        for index, row in negative_exact_kws_data.iterrows():
            negative_exact_kw_upload_format_temp = pd.DataFrame([[None] * len(std_columns)], columns=std_columns)
            negative_exact_kw_upload_format_temp['Campaign Name'] = row['Campaign Name']
            negative_exact_kw_upload_format_temp['Ad Group Name'] = row['Ad Group Name']
            negative_exact_kw_upload_format_temp['Keyword'] = row['Customer Search Term']
            negative_exact_kws_upload_format = negative_exact_kws_upload_format.append(
                negative_exact_kw_upload_format_temp)
        negative_exact_kws_upload_format['Match Type'] = 'negative exact'
        negative_exact_kws_upload_format[['Campaign Status', 'Ad Group Status', 'Status']] = 'enabled'

        # 由于ad group name列存在命名不规范的情况(ad group name命名规范为sku asin),于是重新调整ad group的命名

        # 分开处理否定关键词和否定ASIN,由于否定ASIN需要打在定向ASIN下面打,于是改变否定ASIN的输出格式
        camp_data_sku_info = camp_data[['Campaign', 'Ad Group', 'SKU']][pd.notnull(camp_data['SKU'])]
        active_listing_data_sku_asin = active_listing_data[['seller-sku', 'asin1']]
        # 得到正确的SKU,添加SKU列
        negative_exact_kws_upload_format = pd.merge(negative_exact_kws_upload_format, camp_data_sku_info,
                                                    left_on=['Campaign Name', 'Ad Group Name'],
                                                    right_on=['Campaign', 'Ad Group'], how='left')
        # 得到正确的ASIN,添加ASIN列
        negative_exact_kws_upload_format = pd.merge(negative_exact_kws_upload_format, active_listing_data_sku_asin,
                                                    left_on=['SKU_y'], right_on=['seller-sku'], how='left')
        # negative_exact_kws_upload_format['Ad Group Name'] = negative_exact_kws_upload_format['SKU_y'] + ' ' + \
        #                                                     negative_exact_kws_upload_format['asin1']
        negative_exact_kws_upload_format.rename(columns={'SKU_x': 'SKU'}, inplace=True)
        negative_exact_kws_upload_format = negative_exact_kws_upload_format[
            pd.notnull(negative_exact_kws_upload_format['Ad Group Name'])]
        negative_exact_kws_upload_format = negative_exact_kws_upload_format[std_columns]

        if not negative_exact_kws_upload_format.empty:

            # 精否词数据
            negative_exact_kw = negative_exact_kws_upload_format[
                ~negative_exact_kws_upload_format['Keyword'].str.contains('b0', case=False)]


            # 否定ASIN
            negative_asin_all_rows = negative_exact_kws_upload_format[
                negative_exact_kws_upload_format['Keyword'].str.contains('b0', case=False)]

        else:
            return

        # 每个否定kw行添加ad group
        if not negative_exact_kw.empty:

            def build_negative_kw_format(negative_kw_row:pd.Series,campaign):
                """
                为每一个否定kw行添加对应的ad group行
                :param negative_kw_row:
                :return:
                """

                camp_ad_group_data = campaign[campaign['Record Type'] == 'Ad Group']
                campaign_name = negative_kw_row['Campaign Name']
                ad_group_name =negative_kw_row['Ad Group Name']
                sku = ad_group_name.split(' ')[0]
                ad_group_row = camp_ad_group_data[(camp_ad_group_data['Campaign'] == campaign_name) & (camp_ad_group_data['Ad Group'] == ad_group_name)]
                if not ad_group_row.empty:
                    ad_group_bid = ad_group_row['Max Bid'].values[0]
                    ad_group_row = [campaign_name, None, None, None, None, ad_group_name, ad_group_bid, None,
                                    None, None, None, 'enabled', 'enabled', None, None]
                    # sku_row =  [campaign_name, None, None, None, None, ad_group_name, None, sku,
                    #                 None, None, None, 'enabled', 'enabled', 'enabled', None]
                else:
                    # defalut ad group row
                    site = station_name[-2:].upper()
                    min_bid = ad_group_max_bid_lower_limit_dict[site]
                    ad_group_row = ['MANUAL-ST-EXACT-by-SP_Bulk',None,None,None,None,ad_group_name,min_bid,None,None,None,None,'enabled','enabled',None,None]
                    # sku_row = ['MANUAL-ST-EXACT-by-SP_Bulk', None, None, None, None, ad_group_name, None, sku,
                    #                 None, None, None, 'enabled', 'enabled', 'enabled', None]
                ad_group_row = pd.Series(ad_group_row,index=negative_kw_row.index)
                # sku_row = pd.Series(sku_row, index=negative_kw_row.index)
                # one_negative_kw_group = pd.concat([ad_group_row,sku_row,negative_kw_row],axis=1).T
                one_negative_kw_group = pd.concat([ad_group_row, negative_kw_row], axis=1).T
                return one_negative_kw_group

            negative_exact_kw = list(
                map(build_negative_kw_format, [negative_kw_row for _, negative_kw_row in
                                                 negative_exact_kw.iterrows()],
                    [camp_data] * len(negative_exact_kw)))

            negative_exact_kw = pd.concat(negative_exact_kw)



        if not negative_asin_all_rows.empty:

            # 否定asin中添加ad行,sku行和定向asin行
            def bulid_negative_asin_format(negative_asin_row: 'pd.Series', station_site,
                                           unknown_station_ad_group_max_bid_lower_limit=0.02):
                """
                否定asin为一行,添加对应的ad gruop行,sku行以及创造一个自身对应的定向asin
                ,同时出价给站点的最小出价
                :param negative_asin_row: 一个否定asin行
                :return: 充填完全的一个asin
                """
                station_site_list = ad_group_max_bid_lower_limit_dict.keys()

                if station_site.upper() in station_site_list:
                    station_ad_group_max_bid_lower_limit = ad_group_max_bid_lower_limit_dict[station_site]

                else:
                    print(f"UNKNOWN SITE: {station_site} 未知.")
                    print("广告组出价暂时给0.02,请及时添加新站点信息")
                    station_ad_group_max_bid_lower_limit = unknown_station_ad_group_max_bid_lower_limit

                # campaign name为固定值
                campaign_name = 'Negative Targeting Expression-SP_Bulk'
                ad_group_name = negative_asin_row['Ad Group Name']
                negative_asin_row_index = negative_asin_row.index
                empty_row = [None] * len(negative_asin_row)
                # find asin
                asin = re.findall('[Bb]0.{8}', ad_group_name)
                # 若没有asin,则返回
                if not asin:
                    return pd.Series(empty_row, index=negative_asin_row_index)
                ad_group_asin_expression = f'ASIN="{asin[-1].upper()}"'
                sku = ad_group_name.split(' ')[0]
                # ad_group_name是sku加asin
                ad_group_name = f'{sku} {asin[-1]}'
                empty_row = [None] * len(negative_asin_row)
                # first row(ad 行)
                ad_group_row = pd.Series(empty_row, index=negative_asin_row_index)
                ad_group_row['Max Bid'] = station_ad_group_max_bid_lower_limit
                ad_group_row['Campaign Status'] = 'enabled'
                ad_group_row['Ad Group Status'] = 'enabled'
                # second row(sku 行)
                sku_row = pd.Series(empty_row, index=negative_asin_row_index)
                sku_row['SKU'] = sku
                sku_row['Campaign Status'], sku_row['Ad Group Status'], sku_row[
                    'Status'] = 'enabled', 'enabled', 'enabled'
                # third row(创造的定向asin行)
                create_asin_row = pd.Series(empty_row, index=negative_asin_row_index)
                create_asin_row['Max Bid'] = station_ad_group_max_bid_lower_limit
                create_asin_row['Keyword'], create_asin_row[
                    'Product Targeting ID'] = ad_group_asin_expression, ad_group_asin_expression
                create_asin_row['Match Type'] = 'Targeting Expression'
                create_asin_row['Campaign Status'], create_asin_row['Ad Group Status'], create_asin_row[
                    'Status'] = 'enabled', 'enabled', 'enabled'
                # fourth row(第四行为否定asin行)
                negative_asin = negative_asin_row['Keyword']
                negative_asin_expression = f'ASIN="{negative_asin.upper()}"'
                negative_asin_row['Keyword'], negative_asin_row[
                    'Product Targeting ID'] = negative_asin_expression, negative_asin_expression
                negative_asin_row['Match Type'] = 'Negative Targeting Expression'

                # 合并四行
                one_negative_asin_df = pd.concat([ad_group_row, sku_row, create_asin_row, negative_asin_row], axis=1).T
                # 添加Campaign 行和ad group行
                one_negative_asin_df['Campaign Name'] = campaign_name
                one_negative_asin_df['Ad Group Name'] = ad_group_name
                return one_negative_asin_df

                # # 否定ASIN的Campaign Name是固定写法"Negative Targeting Expression-SP_Bulk"

            station_site = station_name[-2:].upper()
            # 循环否定asin中的每一行
            all_format_negative_asin = list(
                map(bulid_negative_asin_format, [negative_asin_row for _, negative_asin_row in
                                                 negative_asin_all_rows.iterrows()],
                    [station_site] * len(negative_asin_all_rows)))
            all_format_negative_asin = pd.concat(all_format_negative_asin)
            all_negative_data = pd.concat([negative_exact_kw, all_format_negative_asin])

        else:
            all_negative_data = negative_exact_kw
        all_negative_data.drop_duplicates(inplace=True)
        all_negative_data.reset_index(drop=True, inplace=True)

        return all_negative_data

    # 生成精否过程表
    negative_exact_kws_temp = get_negative_exact_kws(station_name, st_data, camp_data)

    # 生成精否表
    all_negative_data = negative_kws_file(station_name, negative_exact_kws_temp, camp_data, active_listing_data)

    # 由于erp上传必须要ad group行,于是添加ad group行

    return all_negative_data



if __name__ == "__main__":
    st_path = r"C:\Users\Administrator\Desktop\rows\Sponsored Products Search term report.xlsx"
    camp_path = r"C:\Users\Administrator\Desktop\rows\es-al0lurn6j8tvb-30天-1590477333686.xlsx"
    ac_path = r"C:\Users\Administrator\Desktop\rows\Active+Listings+Report+05-26-2020.txt"
    ao_path = r"C:\Users\Administrator\Desktop\rows\21881814322018408.txt"
    ac_data = process_files.read_file(ac_path)
    print(f'ac_data:{ac_data.shape}')
    st_data = process_files.read_file(st_path,sheet_name='Sponsored Product Search Term R')
    print(f'st_data:{st_data.shape}')
    camp_data = process_files.read_file(camp_path, sheet_name='Sponsored Products Campaigns')
    ao_data = process_files.read_file(ao_path)
    station_name = 'XUNOOO_ES'
    all_negative_data = negative_exact(station_name, camp_data, st_data, ac_data)
    all_negative_data.to_excel(r'C:\Users\Administrator\Desktop\精否out.xlsx')
