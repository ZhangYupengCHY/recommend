import pandas as pd
import numpy as np


# 导入sku和kw数据表
def sku_kws_data(path):
    data = pd.read_excel(path)
    data.columns = [column.upper() for column in list(data.columns)]
    data = data[['SKU', 'KEYWORD']]
    return data


# 将sku和kw数据表分组
def group_sku_and_kws(data):
    global sep_word
    sep_word = 'sep'
    data.fillna(sep_word, inplace=True)
    sep = [0]
    for i in range(1, data.shape[0] - 1):
        if (data.ix[i, 'SKU'] == sep_word) & (data.ix[i + 1, 'SKU'] != sep_word):
            sep.append(i)
        if (data.ix[i, 'SKU'] == sep_word) & (data.ix[i + 1, 'SKU'] == sep_word) & (data.ix[i - 1, 'SKU'] != sep_word):
            last_sku = i - 1
    my_dict = {}
    len_sep = len(sep)
    if len_sep>1:
        my_dict[0] = data.ix[sep[0]:sep[1] - 1, :]
        for i in range(2, len_sep):
            my_dict[i] = data.ix[sep[i - 1] + 1:sep[i] - 1, :]
        my_dict[len_sep] = data.ix[sep[-1] + 1:last_sku, :]
    else:
        my_dict[0] = data
    return my_dict


# 计算每个店铺的每天的预算
def camp_daily_budget(all_info, site, one_sku_budget,max_budget=20):
    len_sku = all_info['SKU'][all_info['SKU'].notnull()].count()
    camp_budget = round(min(len_sku * one_sku_budget, max_budget) / change_current[site], 2)
    return camp_budget


# 计算sku与kw
def calc_sku_and_kws(base_dir,account, site,camp_name,ad_group_name,ad_manger,one_sku_budget,one_kw_price,path
                     ):
    global change_current, my_columns, campaign_name_info
    # 表的列名
    my_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date', \
                  'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'sku', 'Keyword', 'Match Type', \
                  'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']
    # 汇率
    change_current = {'US': 1, 'CA': 1, 'UK': 1, 'DE': 1, 'IT': 1,
                      'ES': 1, 'JP': 0.008997, 'FR': 1, 'MX': 0.0513,
                      'IN': 0.01418, 'AU': 1}
    # sku以及kws信息
    sku_info = sku_kws_data(base_dir)
    # 计算店铺的预算
    camp_budget = camp_daily_budget(sku_info, site,one_sku_budget)
    # 所有的信息
    empty_list = [np.nan] * len(my_columns)
    first_info = pd.DataFrame([empty_list], columns=my_columns)
    campaign_name_info = '清仓_MANUAL-{}_{}-by-SP_Bulk'.format(account, site)
    # all_info['Campaign Name'] = campaign_name_info
    first_info.ix[0, 'Campaign Targeting Type'] = 'MANUAL'
    first_info.ix[0, 'Campaign Daily Budget'] = camp_budget
    # all_info['Ad Group Name'] = ad_group_name
    first_info.ix[0, 'Campaign Status'] = 'enable'
    # 将sku分组分开（按导入的excel文件中的空格分组）
    grouped_sku = group_sku_and_kws(sku_info)
    all_rest_info = pd.DataFrame(columns=my_columns)
    for value in grouped_sku.values():
        if value.empty:
            continue
        # ad_group 第一行数据（第一部分）
        empty_list = [np.nan] * len(my_columns)
        first_row = pd.DataFrame([empty_list], columns=my_columns)
        first_row['Max Bid'] = round(0.02 / change_current[site], 2)
        first_row['Ad Group Status'] = 'enable'
        # ad_group 第二部分数据 sku
        second_part = pd.DataFrame(columns=my_columns)
        second_part['sku'] = value['SKU']
        second_part['Status'] = 'enable'
        # ad_group 第三部分  出价和kw
        third_part = pd.DataFrame(columns=my_columns)
        third_part['Keyword'] = value['KEYWORD'][value['KEYWORD'] != sep_word]
        third_part.reset_index(drop=True, inplace=True)
        third_part['Max Bid'] = third_part['Keyword'].apply(
            lambda x: round(min(len(x.split(' ')) * one_kw_price, 0.25) / change_current[site], 2))
        # third_part['Max Bid'] = round(
        #     min(len(third_part.ix[0, 'Keyword'].split(' ')) * 0.05, 0.25) / change_current[site], 2)
        third_part['Match Type'] = 'phrase'.upper()
        third_part['Status'] = 'enable'
        rest1_info = pd.concat([first_row, second_part, third_part])
        all_rest_info = pd.concat([all_rest_info, rest1_info])
        all_rest_info['Ad Group Name'] = ad_group_name
    all_info = pd.concat([first_info, all_rest_info])
    all_info['Campaign Name'] = campaign_name_info
    all_info = all_info[all_info['sku'] != sep_word]
    all_info.to_excel(path, index=False)


if __name__ == '__main__':
    path = r'C:\Users\Administrator\Desktop\CAREDY_DE\CAREDY_DE-KW-2019-10-19 18-11-47.xlsx'
    out_path = r'C:\Users\Administrator\Desktop\out.xlsx'
    calc_sku_and_kws(path,'kimiss', 'UK', '清仓_manual_kimiss_sp','MANUAL_KWS','WY',1,0.05,out_path)

