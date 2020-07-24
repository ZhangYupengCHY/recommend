# -*- coding: utf-8 -*-
"""
Created on Tue Oct  8 11:37:43 2019

@author: Administrator
"""

import pandas as pd
import math
import datetime
import os


# 导入广告对接人信息
def import_guy_charge_camp():
    path = r'C:\Users\Administrator\Desktop\清仓\站点对应广告对接人.xlsx'
    data = pd.read_excel(path, sheet_name='Sheet1')
    data.drop_duplicates(inplace=True)
    return data


# 导入acos与click参考表来浮动cpc
def load_acos_click():
    path = r'C:\Users\Administrator\Desktop\清仓\按acos click两次浮动.xlsx'
    acos = pd.read_excel(path, sheet_name='acos')
    click = pd.read_excel(path, sheet_name='click')
    return [acos, click]


# 对acos表做处理，得到最小的acos和最大的acos,左闭右开
def process_acos(df):
    df['acos'] = df['acos'].apply(lambda x: (x.strip('[').strip(')').strip('(')))
    df['min_acos'] = df['acos'].apply(lambda x: int(x.split(',')[0].strip('%')) / 100)
    df['max_acos'] = df['acos'].apply(lambda x: int(x.split(',')[1].strip('%')) / 100)


# 对click表做处理，得到最小的click和最大的click,左闭右开
def process_click(df):
    df['click'] = df['click'].apply(lambda x: (x.strip('[').strip(')').strip('(')))
    df['min_click'] = df['click'].apply(lambda x: int(x.split(',')[0]))
    df['max_click'] = df['click'].apply(lambda x: int(x.split(',')[1]))


# 找到每个sku的手动出价进行调价，其中对cpc参照acos范围和click范围进行调价
def calc_manual_sku_bid(df):
    df['click'] = df['点击量'] / df['订单量']
    [acos, click] = load_acos_click()
    process_acos(acos)
    process_click(click)
    # 按照acos进行一次调价
    for i in range(len(df)):
        df_acos = df.ix[i, 'ACoS']
        rate = acos['Broad_Acos'][(acos['min_acos'] <= df_acos) & (acos['max_acos'] > df_acos)].values[0]
        if df.ix[i, 'CPC'] * (rate - 1) >= 0:
            df.ix[i, 'CPC_change1'] = df.ix[i, 'CPC'] + min(abs(df.ix[i, 'CPC'] * (rate - 1)), abs(
                acos['Broad'][(acos['min_acos'] <= df_acos) & (acos['max_acos'] > df_acos)].values[0]))
        else:
            df.ix[i, 'CPC_change1'] = df.ix[i, 'CPC'] - min(abs(df.ix[i, 'CPC'] * (rate - 1)), abs(
                acos['Broad'][(acos['min_acos'] <= df_acos) & (acos['max_acos'] > df_acos)].values[0]))

    # 按照click进行二次调价
    for i in range(len(df)):
        df_click = df.ix[i, 'click']
        rate = click['Broad_Acos'][(click['min_click'] <= df_click) & (click['max_click'] > df_click)].values[0]
        if df.ix[i, 'CPC_change1'] * (rate - 1) >= 0:
            df.ix[i, 'CPC_change2'] = df.ix[i, 'CPC_change1'] + min(abs(df.ix[i, 'CPC_change1'] * (rate - 1)), abs(
                click['Broad'][(click['min_click'] <= df_click) & (click['max_click'] > df_click)].values[0]))
        else:
            df.ix[i, 'CPC_change2'] = df.ix[i, 'CPC_change1'] - min(abs(df.ix[i, 'CPC_change1'] * (rate - 1)), abs(
                click['Broad'][(click['min_click'] <= df_click) & (click['max_click'] > df_click)].values[0]))
    df['CPC_change2'] = df['CPC_change2'].apply(lambda x: math.floor(x * 100) / 100)
    return df


# 筛选出出单的sku
def have_order_sku():
    path = r'C:\Users\Administrator\Desktop\清仓\order_kw_db.xlsx'
    data = pd.read_excel(path, sheet_name='order_kw_db')
    data.drop_duplicates(subset=['SKU', 'Ad Group Name', 'Customer Search Term'], keep='last', inplace=True)
    guys_incharge_camp = import_guy_charge_camp()
    # guys_incharge_camp.sort_values(['station'],inplace=True)
    all_high_dgr = import_data()
    all_high_dgr = pd.merge(all_high_dgr, guys_incharge_camp, how='left', left_on='camp_site', right_on='station')
    all_high_dgr['ad_manger'][all_high_dgr['ad_manger'].isna()] = 'Nobody'
    have_ordered_sku = pd.merge(all_high_dgr, data[['SKU', 'Customer Search Term', '点击量', '订单量', 'CPC', 'ACoS']],
                                how='inner', left_on='seller_sku', right_on='SKU')
    have_ordered_sku = calc_manual_sku_bid(have_ordered_sku)
    return have_ordered_sku


# 导入清仓数据
def import_data():
    path = r'C:\Users\Administrator\Desktop\清仓\清仓方案.xlsx'
    data = pd.read_excel(path, sheet_name=0)
    # data = select_data(data)
    data['账号'] = data['账号'].apply(lambda x: x.lower())
    country_exchange = {'澳大利亚': 'au', '德国': 'de', '法国': 'fr', '加拿大': 'ca', '美国': 'us', '墨西哥': 'mx', '日本': 'jp',
                        '西班牙': 'es', '意大利': 'it', '英国': 'uk'}
    data['site'] = data['国家'].apply(lambda x: country_exchange[x])
    data['camp_site'] = data['账号'] + '_' + data['site']
    return data


# 筛选出库存数量等级大于等于A7，货本等级大于等于B4，单价等级大于等于C7
def select_data(data):
    qty_deg = ['A4', 'A5', 'A6', 'A7', 'A8', 'A9', 'A10']
    stg_deg = ['B3', 'B4', 'B5', 'B6']
    sale_deg = ['C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11']
    new_data = data[(data['库存数量等级'].isin(qty_deg)) | (data['货本等级'].isin(stg_deg)) | (data['单价等级'].isin(sale_deg))]
    return new_data


# 计算每个广告组的单日限价
def camp_daily_bid(df, every_sku_total_bid=5):
    daily_bid = df['Ad Group Name'].count()
    return daily_bid * every_sku_total_bid / 2


# 手动广告计算单个广告组的信息
def process_manual_sku_data(all_df, acos_rate=0.2, cr=0.03, min_bid=0.02):
    df = all_df
    df.reset_index(inplace=True)
    country_dict = {'澳大利亚': 'AU', '德国': 'DE', '法国': 'FR', '加拿大': 'CA', '美国': 'US', \
                    '墨西哥': 'MX', '日本': 'JP', '西班牙': 'ES', '意大利': 'IT', '英国': 'UK'}
    country_max_bid = {'US': 0.45, 'UK': 0.45, 'CA': 0.3, 'DE': 0.25, 'IT': 0.25, 'FR': 0.25, 'ES': 0.25, 'JP': 30,
                       'MX': 3, 'AU': 0.45}
    this_country = country_dict[df.ix[0, '国家']]
    campaign_name = '清仓_MANUAL-{}_{}-by-SP_Bulk'.format(df.ix[0, '账号'], this_country)
    # daily_budget = budget
    targeting_type = 'MANUAL'
    campaign_status = 'enabled'
    exchange_rate = {'A1': 1.1, 'A2': 1.1, 'A3': 1.2, 'A4': 1.2, 'A5': 1.3, 'A6': 1.3, 'A7': 1.4, 'A8': 1.4, 'A9': 1.5,
                     'A10': 1.5}
    processed_account_ad_group = pd.DataFrame(columns=my_columns)
    processed_account_sku = pd.DataFrame(columns=my_columns)
    ad_group_name = df['seller_sku'] + ' ' + df['asin'] + '-' + df['库存数量等级'] + '_' + df[
        '货本等级'] + '_' + df['单价等级']
    df['库存数量等级值'] = [exchange_rate[i] for i in df['库存数量等级']]
    df['库存数量等级值'] = df['库存数量等级值'].astype(object)
    processed_account_ad_group['Ad Group Name'] = ad_group_name
    processed_account_ad_group['Max Bid'] = acos_rate * cr * df['库存数量等级值'] * df['预计此次调价结果--最终售价(本币)']
    processed_account_ad_group['Max Bid'] = processed_account_ad_group['Max Bid'].apply(
        lambda x: math.ceil(x * 100) / 100)
    for i, bid in enumerate(processed_account_ad_group['Max Bid']):
        if (bid <= 0.02) & (bid >= 0):
            processed_account_ad_group['Max Bid'].iloc[i] = min_bid
        elif bid >= country_max_bid[this_country]:
            processed_account_ad_group['Max Bid'].iloc[i] = country_max_bid[this_country]
    processed_account_ad_group['Campaign Name'] = campaign_name
    processed_account_ad_group['Ad Group Status'] = 'enabled'
    processed_account_sku['sku'] = df['seller_sku']
    processed_account_sku['Status'] = 'enabled'
    processed_account_sku['Campaign Name'] = campaign_name
    processed_account_sku['Ad Group Name'] = ad_group_name
    temp_info = pd.DataFrame(columns=my_columns)
    for i in range(len(processed_account_sku)):
        one_row = pd.concat([processed_account_ad_group.ix[i:i, :], processed_account_sku.ix[i:i, :]])
        temp_info = pd.concat([temp_info, one_row])
    # temp_info.sort_values(by=['Ad Group Name', 'Max Bid'], inplace=True)
    temp_info.drop_duplicates(inplace=True)
    # 每个广告组的第一行的数据
    agg_camp = camp_daily_bid(temp_info)
    # 由于每一个店铺一个广告组，于是出价设置为固定的10元
    # agg_camp = 10
    first_column = {'Campaign Name': campaign_name, 'Campaign Daily Budget': agg_camp, 'Campaign Start Date': '', \
                    'Campaign End Date': '', 'Campaign Targeting Type': targeting_type, 'Ad Group Name': '', \
                    'Max Bid': '', 'sku': '', 'Keyword': '', 'Match Type': '', 'Campaign Status': campaign_status,
                    'Ad Group Status': '', \
                    'Status': '', 'Bidding strategy': ''}
    all_info = pd.DataFrame(columns=my_columns)
    # all_info = pd.concat([pd.DataFrame([list(first_column.values())], columns=my_columns), temp_info])
    all_info = temp_info
    all_info.reset_index(drop=True, inplace=True)
    # =============================================================================
    #     for i in range(int(len(all_info) / 3)):
    #         all_info.ix[3 * i, 'Campaign Name'] = all_info.ix[((3 * i) + 1), 'Campaign Name']
    #         all_info.ix[3 * i, 'Ad Group Name'] = all_info.ix[((3 * i) + 1), 'Ad Group Name']
    # =============================================================================
    # =============================================================================
    #     all_info.ix[0, 'Campaign Name'] = all_info.ix[1, 'Campaign Name']
    #     all_info.ix[0, 'Ad Group Name'] = all_info.ix[1, 'Ad Group Name']
    # =============================================================================
    df2 = all_df.ix[:, 33:]
    sku_info = pd.DataFrame(columns=my_columns)
    sku_info['Max Bid'] = df2['CPC_change2']
    sku_info['Keyword'] = df2['Customer Search Term']
    sku_info['Match Type'] = 'BROAD'
    sku_info['Status'] = 'enabled'
    sku_info['Campaign Name'] = campaign_name
    sku_info['Ad Group Name'] = ad_group_name
    all_info = pd.concat([all_info, sku_info])
    all_info.sort_values(['Ad Group Name', 'Ad Group Status', 'sku', 'Keyword'], inplace=True)
    all_info = pd.concat([pd.DataFrame([list(first_column.values())], columns=my_columns), all_info])
    return all_info


# 自动广告计算单个广告组的信息
def process_auto_sku_data(df, acos_rate=0.2, cr=0.03, min_bid=0.02):
    df.reset_index(inplace=True)
    country_dict = {'澳大利亚': 'AU', '德国': 'DE', '法国': 'FR', '加拿大': 'CA', '美国': 'US', \
                    '墨西哥': 'MX', '日本': 'JP', '西班牙': 'ES', '意大利': 'IT', '英国': 'UK'}
    country_max_bid = {'US': 0.45, 'UK': 0.45, 'CA': 0.3, 'DE': 0.25, 'IT': 0.25, 'FR': 0.25, 'ES': 0.25, 'JP': 30,
                       'MX': 3, 'AU': 0.45}
    this_country = country_dict[df.ix[0, '国家']]
    campaign_name = '清仓_AUTO-{}_{}-by-SP_Bulk'.format(df.ix[0, '账号'], this_country)
    # daily_budget = budget
    targeting_type = 'AUTO'
    campaign_status = 'enabled'
    exchange_rate = {'A1': 1.1, 'A2': 1.1, 'A3': 1.2, 'A4': 1.2, 'A5': 1.3, 'A6': 1.3, 'A7': 1.4, 'A8': 1.4, 'A9': 1.5,
                     'A10': 1.5}
    processed_account_ad_group = pd.DataFrame(columns=my_columns)
    processed_account_sku = pd.DataFrame(columns=my_columns)
    ad_group_name = df['seller_sku'] + ' ' + df['asin'] + '-' + df['库存数量等级'] + '_' + df[
        '货本等级'] + '_' + df['单价等级']
    df['库存数量等级值'] = [exchange_rate[i] for i in df['库存数量等级']]
    df['库存数量等级值'] = df['库存数量等级值'].astype(object)
    processed_account_ad_group['Ad Group Name'] = ad_group_name
    processed_account_ad_group['Max Bid'] = acos_rate * cr * df['库存数量等级值'] * df['预计此次调价结果--最终售价(本币)']
    processed_account_ad_group['Max Bid'] = processed_account_ad_group['Max Bid'].apply(
        lambda x: math.ceil(x * 100) / 100)
    for i, bid in enumerate(processed_account_ad_group['Max Bid']):
        if (bid <= 0.02) & (bid >= 0):
            processed_account_ad_group['Max Bid'].iloc[i] = min_bid
        elif bid >= country_max_bid[this_country]:
            processed_account_ad_group['Max Bid'].iloc[i] = country_max_bid[this_country]
    processed_account_ad_group['Campaign Name'] = campaign_name
    processed_account_ad_group['Ad Group Status'] = 'enabled'
    processed_account_sku['sku'] = df['seller_sku']
    processed_account_sku['Status'] = 'enabled'
    processed_account_sku['Campaign Name'] = campaign_name
    processed_account_sku['Ad Group Name'] = ad_group_name
    temp_info = pd.DataFrame(columns=my_columns)
    for i in range(len(processed_account_sku)):
        one_row = pd.concat([processed_account_ad_group.ix[i:i, :], processed_account_sku.ix[i:i, :]])
        temp_info = pd.concat([temp_info, one_row])
    # temp_info.sort_values(by=['Ad Group Name', 'Max Bid'], inplace=True)
    # 每个广告组的第一行的数据
    agg_camp = camp_daily_bid(temp_info)
    # 由于每一个店铺一个广告组，于是出价设置为固定的10元
    # agg_camp = 10
    first_column = {'Campaign Name': campaign_name, 'Campaign Daily Budget': agg_camp, 'Campaign Start Date': '', \
                    'Campaign End Date': '', 'Campaign Targeting Type': targeting_type, 'Ad Group Name': '', \
                    'Max Bid': '', 'sku': '', 'Keyword': '', 'Match Type': '', 'Campaign Status': campaign_status,
                    'Ad Group Status': '', \
                    'Status': '', 'Bidding strategy': ''}
    # all_info = pd.DataFrame(columns=my_columns)
    all_info = pd.concat([pd.DataFrame([list(first_column.values())], columns=my_columns), temp_info])
    all_info.reset_index(drop=True, inplace=True)
    return all_info


def sepr_guys(auto_data, manual_data):
    camps_sites = set(auto_data['camp_site'])
    for camp_site in camps_sites:
        camp_site_auto = auto_data[auto_data['camp_site'] == camp_site]
        charge_guy = auto_data['ad_manger'][auto_data['camp_site'] == camp_site].values[0]
        camp = camp_site[:-3]
        site = camp_site[-2:]
        auto_info = process_auto_sku_data(camp_site_auto)
        if len(manual_data[manual_data['camp_site'] == camp_site]):
            camp_site_manual = manual_data[manual_data['camp_site'] == camp_site]
            manual_info = process_manual_sku_data(camp_site_manual)
            all_info = pd.concat([auto_info, manual_info])
        else:
            all_info = auto_info
        if site == 'jp':
            all_info['Campaign Name'] = all_info['Campaign Name'].apply(lambda x: 'QC' + x[2:])
        path = 'C:/Users/Administrator/Desktop/清仓/out1/{}/{}_{}'.format(charge_guy, camp, charge_guy)
        if not os.path.exists(path):
            os.makedirs(path)
        path1 = r'C:\Users\Administrator\Desktop\清仓\out1\{}\{}_{}\{}.xlsx'.format(charge_guy, camp, charge_guy,
                                                                                  camp_site)
        all_info.to_excel(path1, index=False)


def main():
    global countrys, guys_incharge_camp, my_columns
    my_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date', \
                  'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'sku', 'Keyword', 'Match Type', \
                  'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']
    guys_incharge_camp = import_guy_charge_camp()
    # guys_incharge_camp.sort_values(['station'],inplace=True)
    my_data = import_data()
    auto_data = pd.merge(my_data, guys_incharge_camp, how='left', left_on='camp_site', right_on='station')
    auto_data['ad_manger'][auto_data['ad_manger'].isna()] = 'Nobody'

    manual_data = have_order_sku()

    sepr_guys(auto_data, manual_data)


if __name__ == "__main__":
    t0 = datetime.datetime.now()
    main()
    t1 = datetime.datetime.now()
    print((t1 - t0).seconds)
