# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/4/28 15:22
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import pandas as pd
import os
import warnings
from my_toolkit import process_files

warnings.filterwarnings(action='ignore')

campaign_budget_dict = {'CA': 200, 'DE': 200, 'FR': 200, 'IT': 200, 'SP': 200, 'JP': 20000,
                        'UK': 200, 'MX': 3800, 'IN': 14000, 'US': 200, 'ES': 200, 'AU': 200}


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


# 将新建或是更新的广告数据添加Record ID列、Record Type列、Portfolio ID列后上传到erp
def format_new_st_to_erp(station_name, new_st_data, camp_data):
    """
    其他的新增表的格式都是与st新增差不多，这里取名new_st_data指的就是新增

    将st新增中的表中每一条记录添加campaign中对应的三列：Record ID/Record Type/Portfolio ID
    1.Record ID列:为每一列的ID（唯一）
        其中通过st新增表和camp表的campign name/ad group/sku/match type/Keyword列来匹配
        若存在，则可以得到Record ID,
        若不存在则Record ID为空
    2.Record Type列:为每一列的层级
        第一级:Campaign
        第二级:Ad Group
        第三级:Ad
        第四级:Keyword/Product Targeting
    3.Portfolio ID列:
        恒为空
    :param station_name:站点名
    :param new_st_data:st新增
    :param camp_data:广告报表
    :return:
    """
    if (not isinstance(new_st_data, pd.DataFrame)) or (new_st_data is None) or (new_st_data.empty):
        return
    if (not isinstance(camp_data, pd.DataFrame)) or (camp_data is None) or (camp_data.empty):
        return

    # 处理st新增报表，按照campaign-ad_group为唯一来创建组
    def build_new_st_group(new_st_data_ori: pd.DataFrame) -> pd.DataFrame:
        """
        对ST新增的词，按照campaign-ad_group为唯一来创建组，为erp上传的格式。
        1.删除每个广告组的首行
        2.按照'Campaign Name','Ad Group Name'进行分组
        3.添加每组的首行
        :param new_st_data_ori: 原始的dataframe表
        :return: 处理后的dataframe
        """
        # 判断st报表的有效性
        if (not isinstance(new_st_data_ori, pd.DataFrame)) or (new_st_data_ori is None) or (new_st_data_ori.empty):
            return
        # 1.删除每个广告组的首行
        st_columns = new_st_data_ori.columns
        new_st_data_ori.reset_index(drop=True, inplace=True)
        new_st_data_ori['Campaign Status'] = 'enabled'
        new_st_data_ori['Ad Group Status'] = 'enabled'
        # 得到广告大组的预算
        first_row_campaign_budget = new_st_data_ori.ix[0, 'Campaign Daily Budget']
        if pd.isnull(first_row_campaign_budget) or (first_row_campaign_budget == ''):
            site = station_name[-2:]
            campaign_budget = campaign_budget_dict[site.upper()]
        else:
            campaign_budget = first_row_campaign_budget
        if not isinstance(campaign_budget, (int, float)):
            campaign_budget = int(campaign_budget)
        # 判断'Product Targeting ID'是否存在
        if 'Product Targeting ID' not in st_columns:
            columns_list = list(new_st_data_ori.columns)
            columns_list.insert(10, 'Product Targeting ID')
            new_st_data_ori['Product Targeting ID'] = ''
            # 按照新的columns_list来排序
            new_st_data_ori = new_st_data_ori[columns_list]
            st_columns = new_st_data_ori.columns
        new_st_data_no_campaign = new_st_data_ori[pd.isnull(new_st_data_ori['Campaign Daily Budget'])]
        if new_st_data_no_campaign.empty:
            return
        # 2.按照Campaign Name和Ad Group Name来进行分组以及添加每个组的首行
        new_st_data_no_campaign_grouped = new_st_data_no_campaign.groupby(['Campaign Name', 'Ad Group Name'])
        new_st_data_list = []
        for campaign_n_ad_group, one_campaign_n_ad_group_data in new_st_data_no_campaign_grouped:
            campaign_name = one_campaign_n_ad_group_data['Campaign Name'].values[0]
            one_campaign_n_ad_group_data.drop_duplicates(inplace=True)
            campaign_first_row = pd.DataFrame(
                [[campaign_name, campaign_budget, None, None, None, None, None, None, None, None, None,
                  'enabled', None, None, None]], columns=st_columns)

            # 精否没有ad group 需要手动添加ad group行 通过Max Bid列为空来识别是否拥有ad group行
            ad_group_row_sign = one_campaign_n_ad_group_data[pd.notnull(one_campaign_n_ad_group_data['Max Bid'])]
            if ad_group_row_sign.empty:
                ad_group_name = one_campaign_n_ad_group_data['Ad Group Name'].values[0]
                ad_grop_row = pd.DataFrame(
                    [[campaign_name, None, None, None, None, ad_group_name, None, None, None, None, None,
                      'enabled', 'enabled', None, None]], columns=st_columns)
                campaign_first_row = pd.concat([campaign_first_row, ad_grop_row])

            one_campaign_n_ad_group_data_complete = pd.concat([campaign_first_row, one_campaign_n_ad_group_data])
            # Campaign Daily Budget来排序Campaig第一层级，Status来排序Ad group第二层级,SKU来排序Ad第三层级,Keyword来排序第四层级
            one_campaign_n_ad_group_data_complete['Status'].fillna(value='', inplace=True)
            one_campaign_n_ad_group_data_complete.sort_values(by=['Campaign Daily Budget', 'Status', 'SKU', 'Keyword'],
                                                              ascending=[True, True, True, True], inplace=True)
            new_st_data_list.append(one_campaign_n_ad_group_data_complete)
        new_st_data = pd.concat(new_st_data_list)
        new_st_data.reset_index(inplace=True, drop=True)
        return new_st_data

    # 添加Record Type列
    def add_record_type(format_new_st):
        """
        根据特征为format_new_st 添加record_type列:
        1.当Campaign Daily Budget有值，则为第一层级 Campaign
        2.当Ad Group Status有值，但是Status为空,则为第二层级 Ad Group
        3.当SKU有值，则为第三层级 Ad
        4.Keyword有值，则为第四层级
            将Campaign Targeting Type列填充之后
            当Campaign Targeting Type中为Manual 则 Keyword
            当Campaign Targeting Type中为Auto 则 Product Targeting
        :param format_new_st:需要处理的ST新增的erp上传表
        :return:添加了record type列之后的上传表
        """
        if (not isinstance(format_new_st, pd.DataFrame)) or (format_new_st is None) or (format_new_st.empty):
            return
        format_new_st['Record Type'] = ''
        # 添加第一层级 Campaign
        format_new_st['Record Type'] = ['Campaign' if pd.notnull(budget) else record_type for budget, record_type in
                                        zip(format_new_st['Campaign Daily Budget'], format_new_st['Record Type'])]
        # 添加第二层级
        # Status列之前用''填充过空值
        format_new_st['Record Type'] = ['Ad Group' if pd.notnull(ad_status) & (status == '') else record_type for
                                        ad_status, status, record_type in
                                        zip(format_new_st['Ad Group Status'], format_new_st['Status'],
                                            format_new_st['Record Type'])]
        # 添加第三层级
        format_new_st['Record Type'] = ['Ad' if sku != '' else record_type for sku, record_type in
                                        zip(format_new_st['SKU'], format_new_st['Record Type'])]
        # 添加第四层级
        keyword_sign = ['broad', 'exact', 'negative exact']
        format_new_st['Record Type'] = [
            record_type if keyword == '' else 'Keyword' if match_type.lower() in keyword_sign else 'Product Targeting'
            for
            keyword, match_type, record_type in
            zip(format_new_st['Keyword'], format_new_st['Match Type'], format_new_st['Record Type'])]

        # 由于精否中存在ad group 的bid为空的情况，于是用广告表的Max Bid来充填
        ad_group_bid_null = format_new_st[
            pd.isnull(format_new_st['Max Bid_x']) & (format_new_st['Record Type'] == 'Ad Group')]
        if not ad_group_bid_null.empty:
            format_new_st['Max Bid_x'] = [
                camp_max_bid if (record_type == 'Ad Group') & pd.isnull(st_max_bid) else st_max_bid for
                st_max_bid, camp_max_bid, record_type in
                zip(format_new_st['Max Bid_x'], format_new_st['Max Bid_y'], format_new_st['Record Type'])]

        format_new_st.rename(columns={'Max Bid_x': 'Max Bid'}, inplace=True)
        return format_new_st

    new_st_data = build_new_st_group(new_st_data)

    # 创建辅助列来合并
    new_st_data[['Campaign Name', 'Ad Group Name', 'SKU', 'Match Type', 'Keyword']] = new_st_data[
        ['Campaign Name', 'Ad Group Name', 'SKU', 'Match Type', 'Keyword']].fillna(value='')
    camp_data[['Campaign', 'Ad Group', 'SKU', 'Match Type', 'Keyword or Product Targeting']] = camp_data[
        ['Campaign', 'Ad Group', 'SKU', 'Match Type', 'Keyword or Product Targeting']].fillna(value='')
    new_st_data['aux'] = new_st_data['Campaign Name'] + new_st_data['Ad Group Name'] + new_st_data[
        'SKU'] + new_st_data['Match Type'] + new_st_data['Keyword']
    camp_data['aux'] = camp_data['Campaign'] + camp_data['Ad Group'] + camp_data['SKU'] + camp_data[
        'Match Type'] + camp_data['Keyword or Product Targeting']
    camp_match_data = camp_data[['Record ID', 'Portfolio ID', 'Max Bid', 'aux']]
    camp_match_data.drop_duplicates(inplace=True)

    format_new_st = pd.merge(new_st_data, camp_match_data,
                             on='aux', how='left', sort=False)
    format_new_st.rename(columns={'Campaign Name': 'Campaign', 'Ad Group Name': 'Ad Group'}, inplace=True)

    # 添加record_type列
    format_new_st = add_record_type(format_new_st)

    # 添加bid+列
    format_new_st['Bid+'] = ['off' if (ad_type == 'Campaign') & (pd.isnull(record_id)) else '' for record_id, ad_type in
                             zip(format_new_st['Record ID'], format_new_st['Record Type'])]
    # 填写广告组类型列('Campaign Targeting Type')
    # 先删掉原表中的'Campaign Targeting Type'列,然后再通过内敛，添加Campaign Targeting Type
    del format_new_st['Campaign Targeting Type']
    format_new_st = pd.merge(format_new_st, camp_data[['Record ID', 'Campaign Targeting Type']][
        pd.notnull(camp_data['Campaign Targeting Type'])], on='Record ID',
                             how='left')

    # 调整输出的列顺序
    format_new_st = format_new_st[
        ['Record ID', 'Record Type', 'Campaign', 'Campaign Daily Budget', 'Portfolio ID', 'Campaign Start Date',
         'Campaign End Date', 'Campaign Targeting Type', 'Ad Group', 'Max Bid', 'Keyword', 'Product Targeting ID',
         'Match Type', 'SKU', 'Campaign Status', 'Ad Group Status', 'Status', 'Bid+']]

    return format_new_st


# 新建上传到erp
def uploadErpBuilt():
    new_st_path = r"C:\Users\Administrator\Desktop\erp上传\新建\SINBUY_IT\report\20.05.06 SINBUY_IT 非规范提取关键字.xlsx"
    camp_path = r"C:\Users\Administrator\Desktop\erp上传\新建\SINBUY_IT\it-a1zozu6aqnhwhh-bulksheet-1588732936318.xlsx"
    st_data = read_files(new_st_path)
    camp_data = read_files(camp_path, sheet_name='Sponsored Products Campaigns')
    upload_data = format_new_st_to_erp('SINBUY_IT', st_data, camp_data)
    upload_data.to_excel(
        r"C:\Users\Administrator\Desktop\非规范提取关键字_upload_2_erp_SINBUY_IT.xlsx",
        index=False)


# 将更新的一列(ID)添加到new_st_data
def formatUpdateCampData(stationName, updateData: pd.DataFrame, campData: pd.DataFrame) -> pd.DataFrame:
    """
    若bid有更改，则显示id，若bid没有改变，则不显示id

    Record ID列:为每一列的ID（唯一）
        其中通过st新增表和camp表的campign name/ad group/sku/match type/Keyword列来匹配获得id

    :param stationName:站点名
    :param updateData:需要更新的原始上传表
    :param campData: 广告活动报表
    :return:更新后的需要上传到erp上的上传表
    """
    # 判断上传表数据和广告活动报表数据的有效性
    if (not isinstance(updateData, pd.DataFrame)) or (updateData is None) or (updateData.empty):
        return
    if (not isinstance(campData, pd.DataFrame)) or (campData is None) or (campData.empty):
        return

    # 创建辅助列来匹配ID
    if set(['Campaign Name', 'Ad Group Name', 'SKU', 'Match Type', 'Keyword']).issubset(set(updateData.columns)):
        updateData[['Campaign Name', 'Ad Group Name', 'SKU', 'Match Type', 'Keyword']] = updateData[
            ['Campaign Name', 'Ad Group Name', 'SKU', 'Match Type', 'Keyword']].fillna(value='')
        updateData['aux'] = updateData['Campaign Name'] + updateData['Ad Group Name'] + updateData[
            'SKU'] + updateData['Match Type'] + updateData['Keyword']
    else:
        lostColumns = set(['Campaign Name', 'Ad Group Name', 'SKU', 'Match Type', 'Keyword']) - set(updateData.columns)
        print(f'{stationName} 新建上传表列中缺失{lostColumns}. 请检查列之后再次运行.')
        return
    if set(['Record ID', 'Campaign', 'Ad Group', 'SKU', 'Match Type', 'Keyword or Product Targeting']).issubset(
            set(campData.columns)):
        campData[['Campaign', 'Ad Group', 'SKU', 'Match Type', 'Keyword or Product Targeting']] = campData[
            ['Campaign', 'Ad Group', 'SKU', 'Match Type', 'Keyword or Product Targeting']].fillna(value='')
        campData['aux'] = campData['Campaign'] + campData['Ad Group'] + campData['SKU'] + campData[
            'Match Type'] + campData['Keyword or Product Targeting']
    else:
        lostColumns = set(
            ['Record ID', 'Campaign', 'Ad Group', 'SKU', 'Match Type', 'Keyword or Product Targeting']) - set(
            campData.columns)
        print(f'{stationName} 广告报表列中缺失{lostColumns}. 请检查列之后再次运行.')
        return

    newUpdateData = pd.merge(updateData, campData[['aux', 'Record ID', 'Record Type']], on='aux', how='left')

    if 'Portfolio ID' not in newUpdateData.columns:
        newUpdateData['Portfolio ID'] = ''

    newUpdateData.rename(columns={'Campaign Name': 'Campaign', 'Ad Group Name': 'Ad Group', 'Bidding strategy': 'Bid+'},
                         inplace=True)

    newUpdateData = newUpdateData[
        ['Record ID', 'Record Type', 'Campaign', 'Campaign Daily Budget', 'Portfolio ID', 'Campaign Start Date',
         'Campaign End Date', 'Campaign Targeting Type', 'Ad Group', 'Max Bid', 'Keyword', 'Product Targeting ID',
         'Match Type', 'SKU', 'Campaign Status', 'Ad Group Status', 'Status']]

    return newUpdateData

#
# # 更新上传到erp
# def uploadErpUpdate():
#     new_st_path = r"C:\Users\Administrator\Desktop\CIMENN_DE_2020-05-25_all_create_data.xlsx"
#     camp_path = r"C:\Users\Administrator\Desktop\erp上传\新建\SINBUY_ES\es-a1zozu6aqnhwhh-bulksheet-1588733837486.xlsx"
#     st_data = process_files.read_file(new_st_path)
#     camp_data = process_files.read_file(camp_path, sheet_name='Sponsored Products Campaigns')
#     upload_data = format_new_st_to_erp('CIMENN_DE', st_data, camp_data)
#     upload_data.to_excel(
#         r"C:\Users\Administrator\Desktop\erp上传\新建\精确否定词_upload_2_erp_SINBUY_ES.xlsx",
#         index=False)


if __name__ == "__main__":
    camp_path = r"C:\Users\Administrator\Desktop\上传测试\fr-a1bktjppuuons0-bulksheet-1590369444467.xlsx"
    camp_data = process_files.read_file(camp_path, sheet_name='Sponsored Products Campaigns')
    file_path = r"C:\Users\Administrator\Desktop\精否out.xlsx"
    st_data = process_files.read_file(file_path)
    upload_data = format_new_st_to_erp('yolina_fr', st_data, camp_data)
    upload_data.to_excel(
    r"C:\Users\Administrator\Desktop\精确否定词_upload_2_erp_SINBUY_ES.xlsx",
    index=False)