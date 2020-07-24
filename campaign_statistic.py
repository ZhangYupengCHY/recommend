#!/usr/bin/env python
# coding=utf-8
# author:marmot

import re
from init_proc import change_digital
import pandas as pd
from listing_to_auto_ad import translation
import sys
import os
from init_proc import get_shop_station_from_folder
from connect_database import get_data_from_database
from init_proc import member_judge_mac
from init_proc import init_campaign
from connect_database import insert_acos_into_database,name_dict
import datetime
from addon import file_time_comp
import json
from sql_read_to import sql_write_read
import st_info_get
import poster_large_file
import time
import message_notice
import multiprocessing
import Tkinter as tk
from init_proc import read_campaign
from help import error_folder_postback
from help import write_to_reocrd
import csv
from func_class import curve_generator
reload(sys)
sys.setdefaultencoding('utf-8')
import warnings
warnings.filterwarnings("ignore")

# 站点毛利率字典
focus_acos={'US':'13.5%','CA':'11.6%','MX':'28.2%',
           'UK':'22.4%','FR':'20.4%','DE':'22.8%',
           'ES':'20.2%','IT':'19.9%','JP':'15.6%','AU':'27.8%'}

def score_judge(offset):
    if offset * 100 <= -5:  # 优质
        score, result = 101, u"优质"
    elif -5 < offset * 100 <= -2:  # 良好
        score, result = 100, u"良好"
    elif -2 < offset * 100 <= 0:  # 正常
        score, result = 95, u"正常"
    elif 0 < offset * 100 <= 5:  # 一般
        score, result = 85, u"一般"
    elif offset * 1002 > 5:  # 严重
        score, result = 75, u"严重"
    return [score, result]

# 基于目标acos进行质量得分判断
def acos_judje(campaign,br,station_abbr,shop_station,shop_station_str,cam_path):
    if campaign.empty:
        sales,spend = 0, 0
        acos_percent = 0
        cpc = 0
    else:
        # 从数据库中获取历史acos
        last_acos=get_data_from_database.get_only_station_info([shop_station_str])
        if last_acos=='':last_acos='暂无'
        cpc_dict = translation.cpc_standard
        # acos_dict = translation.upper_acos
        need_cpc=cpc_dict[station_abbr]
        # 获取数据库中的各站点月acos数据
        month_acos_sql = "SELECT ACoS FROM month_acos WHERE 站点='%s'" % station_abbr.upper()
        month_acos_df = sql_write_read.read_table(month_acos_sql)
        need_acos = float(month_acos_df.loc[0, 'ACoS'])
        # print need_acos
        # need_acos=acos_dict[station_abbr]

        need_sum = campaign.loc[0:]
        need_sum = need_sum.loc[(need_sum['Record Type'] == 'Campaign') & (need_sum['Campaign Status'] == 'enabled')]
        need_sum = need_sum.loc[:, ['Spend', 'Sales','Clicks','Orders']]
        sum_data = need_sum.sum()
        spend = round(sum_data['Spend'],2)
        sales = round(sum_data['Sales'],2)
        click = sum_data['Clicks']
        order = sum_data['Orders']
        ad_cr=round(float(order)/click,3)
        ad_cr_per=str(ad_cr*100)+'%'
        cpc = round(float(spend)/click,2)
        offset_cpc = cpc - need_cpc
        if sales!=0: acos = round(float(spend / sales),4)
        else:acos=0
        acos_percent=str(acos*100)+'%'
        offset_acos=acos-need_acos
        offset_percent=str(offset_acos*100)+'%'
        score_now=score_judge(offset_acos)

    if br.empty:
        sale_parcentage = 0
        total_sales = 0
    else:
        # 计算站点总销售额
        total_sales_list=list(br.loc[0:,'Ordered Product Sales'].apply(
            lambda m_data: round(float(reduce(lambda n_data,p_data:n_data+p_data,re.findall('[0-9\.]',str(m_data))).strip('.').strip()),2)))
        total_sales=sum(total_sales_list)
        sale_parcentage_num = round((float(sales) / total_sales) , 2) # 销售额占比小数
        spend_parcentage_num = round((float(spend) / total_sales), 2) # 花费占比小数
        sale_parcentage=str(round(sale_parcentage_num*100,1))+'%'    # 销售额占比
        spend_parcentage=str(round(spend_parcentage_num*100,1))+'%'    # 花费占比

        # 计算店铺转化率
        br_cr=br.loc[0:,['Sessions','Units Ordered']]
        br_cr=br_cr.applymap(lambda m_data:float(str(m_data).replace(',','')))
        br_sum=br_cr.sum()
        session=br_sum['Sessions']
        order=br_sum['Units Ordered']
        if session!=0:
            shop_cr=str((round(order/session,3)*100))+'%'
        else:shop_cr='0%'

    # 回传广告表现数据
    name_abbr = member_judge_mac.member_info_get_name()
    # name_str = name_dict.name_dict.get(name_abbr, 0)
    name_str = name_dict.name_dict_func()
    if name_str:
        name = name_str
    else:
        name = name_abbr
    if not campaign.empty:
        insert_acos_into_database.insert_into_only_station_info(
            [shop_station_str.lower(), name, acos_percent, sales, sale_parcentage, total_sales, cpc])
    else:
        insert_acos_into_database.insert_into_only_station_info(
            [shop_station_str.lower(), name, '', '', '', total_sales, ''])

    if campaign.empty:return []
    # 非规范广告销售额占比太大，先需要清理非规范组，1.在规范组中做重复广告2.关掉非规范中的部分，再新增
    campaigndata1 = campaign.loc[(campaign['Record Type'] == 'Ad')&
                                (campaign['Campaign Status'] == 'enabled')&
                                (campaign['Ad Group Status'] == 'enabled')&
                                (campaign['Status'] == 'enabled')]
    indexdata = []
    for (k1, k2), group in campaigndata1.groupby(['Campaign', 'Ad Group']):
        if len(group.index) != 1:
            indexdata=indexdata+list(group.index)
    campaigndata2 = campaign.loc[indexdata]
    campaigndata2 = campaigndata2.loc[:, ['Spend', 'Sales']]
    nonormal_sum_data = campaigndata2.sum()
    nonormal_spend = round(nonormal_sum_data['Spend'], 2)
    nonormal_sales = round(nonormal_sum_data['Sales'], 2)
    if nonormal_sales!=0:
        nonormal_acos=str(round((nonormal_spend/nonormal_sales)*100,2))+'%'
    else:nonormal_acos='0%'
    if sales == 0:
        nonormal_sale_parcentage = '0%'
    else:
        nonormal_sale_parcentage = str(round((float(nonormal_sales) / sales) * 100, 1)) + '%'  # 非规范组销售额占比
    if total_sales == 0:
        nonormal_spend_parcentage = '0%'
    else:
        nonormal_spend_parcentage = str(round((float(nonormal_spend) / total_sales) * 100, 1)) + '%'  # 非规范组花费占比

    # ACOS=CPC/(CR*PRICE)
    # 1.ACOS高，CPC高--CPC高导致，需降低CPC：
    # 降低新增广告的目标acos；调价后，整体乘以降价系数（0.7-0.9）；降低广告大组预算（近两天大组广告花费）；设置竞价上限（平均cpc4倍）
    # 2.ACOS高，CPC低--广告CR低导致，提升CR或CPC偏高，可适当降低CPC：
    # 做重复广告暂停；做精否；否定asin；做差广告暂停；（可适当降低CPC，参照第一点）
    # 3.ACOS低，CPC高--CR高或客单价高：（可适当降低CPC），参照第一点

    # 广告销售额占比不够20%
    # ACOS低--广告没有完全覆盖：全面新增（可以做active_listing,或者做AO时，客单价限制下调）
    # ACOS高--可能大面积断货（检查卖的好的产品是否大面积断货）；低价做active_listing；了解重点sku的广告效益

    # 广告花费占比太高6%
    # 降低CPC使得ACOS低于综合毛利率（广告花费占比加实际毛利率）
    # 非规范广告销售额占比太大，先需要清理非规范组，1.在规范组中做重复广告2.关掉非规范中的部分，再新增
    if not campaign.empty and not br.empty:
        # 进行ACOS-CPC原因分析
        formula='ACOS=CPC/(CR*PRICE)'
        if offset_acos>0 and offset_cpc>0:
            reason=u'1.ACOS高，CPC高，原因：CPC过高，需降低CPC。'
            method=u'解决方法：降低新增广告的目标acos；调价后，整体乘以降价系数（0.7-0.9）；降低广告大组预算（近两天大组广告花费）；设置竞价上限（平均cpc4倍）。'
        elif offset_acos>0 and offset_cpc<=0:
            reason = u'1.ACOS高，CPC低，原因：广告CR低导致，提升CR(或适当再降低CPC)。'
            method = u'解决方法：做重复广告暂停；做精否；否定asin；做差广告暂停；（可适当降低CPC，' \
                     u'降低新增广告的目标acos；调价后，整体乘以降价系数（0.7-0.9）；降低广告大组预算（近两天大组广告花费）；设置竞价上限（平均cpc4倍）)。'
        elif offset_acos<=0 and offset_cpc>0:
            reason = u'1.ACOS低，CPC高，原因：CR高或客单价高。'
            method = u'解决方法：（可适当降低CPC），降低新增广告的目标acos；调价后，整体乘以降价系数（0.7-0.9）；降低广告大组预算（近两天大组广告花费）；设置竞价上限（平均cpc4倍）。'
        else:
            reason = ''
            method = ''

        # 进行广告销售额占比不够原因分析
        if sale_parcentage_num<0.2 and offset_acos>0:
            reason1 = u'2.广告销售额占比不够，ACOS低：广告没有完全覆盖：全面新增（可以做active_listing,或者做AO时，客单价限制下调）。'
        elif sale_parcentage_num<0.2 and offset_acos<=0:
            reason1 = u'2.广告销售额占比不够，ACOS高：可能大面积断货（检查卖的好的产品是否大面积断货）；' \
                      u'低价做active_listing；了解重点sku的广告效益。'
        else:reason1=''

        # 进行广告花费占比太高原因分析
        if spend_parcentage_num > 0.6:
            reason2 = u'3.广告花费占比太高,降低CPC使得ACOS低于综合毛利率（广告花费占比加实际毛利率）；' \
                      u'非规范结构销售额占比太大，先需要清理非规范组，1.在规范组中做重复广告2.关掉非规范中的部分，再新增。'
        else:
            reason2=''

        # 取出上次分析的acos
        # last_acos_data=pd.read_excel(r"C:\KMSOFT\Ad Helper\station_info\wl_station")
        # last_acos=last_acos_data.loc[]

        #stand_content="1.站点总体状况"
        station_content=[u"打分："+str(score_now[0])+u"分。",
                         u"当前ACoS："+str(acos_percent)+u"，上次操作ACoS："+str(last_acos)+u"，ACoS上限："+str(need_acos * 100) + '%'+u"，偏离"+str(offset_percent)+u"，状态："+str(score_now[1])+u'。',
                         u"平均CPC："+str(cpc)+u"，CPC上限："+str(need_cpc)+u"，偏离"+str(offset_cpc)+u'。',
                         u"广告销售额："+str(sales)+u"，广告花费："+str(spend)+u"，广告转化："+str(ad_cr_per)+u"。站点销售额："+str(total_sales)+u"，站点转化率："+str(shop_cr)+u'。',
                         u"广告销售额占比："+str(sale_parcentage)+u"，广告花费占比："+str(spend_parcentage)+u'。',
                         u"非规范结构销售额："+str(nonormal_sales)+ u"，非规范结构花费："+str(nonormal_spend)+u"，非规范结构ACoS："+str(nonormal_acos)+
                         u"，非规范结构销售额占比："+str(nonormal_sale_parcentage)+u"，非规范结构花费占比："+str(nonormal_spend_parcentage)+u'。',
                         formula,reason,method,reason1,reason2,acos,cpc,sale_parcentage]
        return station_content
    else:
        return []

def ad_num_statistic(campaign):
    campaign_total=campaign.loc[0:]
    # 求广告系列数量
    campaign_enabled=set(campaign_total.loc[(campaign_total['Record Type']=='Campaign')&
                                    (campaign_total['Campaign Status']=='enabled'),'Campaign'])
    campaign_paused=set(campaign_total.loc[(campaign_total['Record Type']=='Campaign')&
                                    (campaign_total['Campaign Status']=='paused'),'Campaign'])
    total_cam= set(campaign_total.loc[(campaign_total['Record Type'] == 'Campaign') ,'Campaign'])

    enabled_num=len(campaign_enabled)
    paused_num = len(campaign_paused)
    total_num=len(total_cam)

    # 求广告组数量
    ad_enabled=list(campaign_total.loc[(campaign_total['Record Type']=='Ad Group')&
                                      (campaign_total['Campaign Status'] == 'enabled')&
                                      (campaign_total['Ad Group Status']=='enabled'),'Ad Group'])
    ad_paused_1 = list(campaign_total.loc[(campaign_total['Record Type'] == 'Ad Group') &
                                        (campaign_total['Campaign Status'] == 'paused')&
                                         (campaign_total['Ad Group Status'] == 'paused'), 'Ad Group'])
    ad_paused_2 = list(campaign_total.loc[(campaign_total['Record Type'] == 'Ad Group') &
                                        (campaign_total['Campaign Status'] == 'enabled')&
                                         (campaign_total['Ad Group Status'] == 'paused'), 'Ad Group'])
    ad_paused_3 = list(campaign_total.loc[(campaign_total['Record Type'] == 'Ad Group') &
                                        (campaign_total['Campaign Status'] == 'paused')&
                                         (campaign_total['Ad Group Status'] == 'enabled'), 'Ad Group'])
    total_ad=list(campaign_total.loc[(campaign_total['Record Type']=='Ad Group'),'Ad Group'])
    enabled_ad_num=len(ad_enabled)
    paused_ad_num = len(ad_paused_1)+len(ad_paused_2)+len(ad_paused_3)
    total_ad_num= len(total_ad)
    content = u"站点共有广告组"+str(total_num)+u"个，开启"+str(enabled_num)+\
              u"个，关闭"+str(paused_num)+u"个。广告"+str(total_ad_num)+\
              u"个，开启"+str(enabled_ad_num)+u"个，关闭"+str(paused_ad_num)+u"个。"
    #stand_content="2.站点广告数量"
    return content

def simplify_name(one_cam):
    ad_manger = member_judge_mac.member_info_get_name()
    if re.search('New',one_cam) and re.search(ad_manger,one_cam) and re.search('AUTO',one_cam):
        simplify_dict='Listing'
    elif re.search(ad_manger,one_cam) and re.search('AUTO',one_cam) and re.search('Special',one_cam):
        simplify_dict = 'Negative'
    elif re.search(ad_manger, one_cam) and re.search('AUTO', one_cam):
        simplify_dict = 'AO'
    elif re.search(ad_manger, one_cam) and re.search('MANUAL', one_cam) and re.search('ST', one_cam):
        simplify_dict = 'ST-EXACT'
    elif re.search(ad_manger, one_cam) and re.search('MANUAL', one_cam):
        simplify_dict = 'ST-BROAD'
    else:
        try:
            # simplify_dict = re.search('[A-Za-z-]{10}',one_cam).group()
            simplify_dict = one_cam
        except:simplify_dict = one_cam
    return simplify_dict

def ad_statistic(campaign,shop_station_str):
    percent_cam = campaign.loc[0:]
    need_sum = percent_cam.loc[(percent_cam['Record Type'] == 'Campaign') &
                                    (percent_cam['Campaign Status'] == 'enabled')]
    need_sum = need_sum.loc[:, ['Campaign','Spend', 'Sales']]

    # 每个campaign的花费和销售额占比
    each_cam_spend={}
    each_cam_sale = {}
    for cam,group in need_sum.groupby('Campaign'):
        group_sum=group.sum()
        group_spend = group_sum['Spend']
        group_sale = group_sum['Sales']
        if group_spend!=0:
            each_cam_spend[cam]=group_spend
        if group_sale != 0:
            each_cam_sale[cam]=group_sale
    base_path=r"C:\KMSOFT\Ad Helper\station_info\pie_img"
    out_path_spend=base_path+"/spend.jpg"
    out_path_sale = base_path + "/sale.jpg"
    all_campaign=set(each_cam_spend.keys())
    simplify_cam1=[]
    simplify_cam2 = []
    spend_value=[]
    sale_value=[]
    for one_cam in all_campaign:
        if one_cam in each_cam_spend.keys():
            simplify_cam1.append(simplify_name(one_cam))
            spend_value.append(each_cam_spend[one_cam])
        if one_cam in each_cam_sale.keys():
            simplify_cam2.append(simplify_name(one_cam))
            sale_value.append(each_cam_sale[one_cam])
    stand_content = "3.站点广告数据"
    return [[out_path_spend,out_path_sale],
            [simplify_cam1,spend_value],
            [simplify_cam2,sale_value]]

def campaign_statistic(campaign):
    stand_content = "4.各广告组数据"

def campaign_cpc(campaign,station_abbr,ad_acos):
    percent_cam = campaign.loc[0:]
    need_sum = percent_cam.loc[(percent_cam['Record Type'] == 'Campaign') &
                               (percent_cam['Campaign Status'] == 'enabled')]
    need_sum = need_sum.loc[:, ['Campaign', 'Spend','Sales', 'Clicks']]

    # 每个campaign的花费和销售额占比
    each_cam_cpc = {}
    all_data=[]
    for cam, group in need_sum.groupby('Campaign'):
        group_sum = group.sum()
        group_spend = round(group_sum['Spend'],2)
        group_sale = round(group_sum['Sales'],2)
        group_click = group_sum['Clicks']
        if group_click!=0:
            cpc=round(float(group_spend)/group_click,2)
        else:cpc=0
        # 算出多余的花费,用销售额指标表示
        acos_dict = translation.upper_acos
        need_acos = acos_dict[station_abbr]
        if need_acos>ad_acos>0:need_acos=ad_acos
        if group_sale!=0:
            acos=str(round((group_spend/group_sale),3)*100)+'%'
        else:acos='0%'
        need_sale = round(group_spend/need_acos,1)
        need_sale_campare = round(group_sale-need_sale,1)

        # each_cam_cpc[simplify_name(cam)] = [group_spend,group_sale,cpc,acos,need_sale_campare]
        all_data.append([simplify_name(cam),str(group_spend),str(group_sale),str(cpc),acos,str(need_sale),need_sale_campare])
    # stand_content = "5.各广告组平均出价"
    data=pd.DataFrame(all_data,columns=['simplify_name','group_spend','group_sale','cpc','acos','need_sale','need_sale_campare'])
    data=data.sort_values(by='need_sale_campare', axis=0, ascending=True)
    data.reset_index(drop=True,inplace=True)
    # print data
    return data

def campaign_cpc_bid(campaign):
    bid_cam = campaign.loc[0:]
    for x, group in bid_cam.groupby('Campaign'):
        bid_cam.loc[group.index, 'style'] =bid_cam.loc[group.index[0], 'Campaign Targeting Type']
    auto_bid = bid_cam.loc[(bid_cam['Record Type'] == 'Ad Group') &
                            (bid_cam['Campaign Status'] == 'enabled')&
                            (bid_cam['Ad Group Status'] == 'enabled')&
                            (bid_cam['Max Bid'] != ' ')&
                            (bid_cam['style'] == 'Auto')]
    manual_bid = bid_cam.loc[(bid_cam['Record Type'] == 'Keyword') &
                            (bid_cam['Campaign Status'] == 'enabled')&
                            (bid_cam['Ad Group Status'] == 'enabled')&
                            (bid_cam['Status'] == 'enabled')&
                            (bid_cam['Max Bid'] != ' ')&
                            (bid_cam['Keyword'] != '*') &
                            (bid_cam['Match Type'].isin(['broad', 'exact', 'phrase']))]

    new_df = auto_bid.append(manual_bid, ignore_index=False)
    if new_df.empty:
        all_cam_ratio = {'no': 'no'}
        return all_cam_ratio
    new_df.loc[:,'CPC']=new_df.apply(lambda m_data:round(m_data['Spend']/m_data['Clicks'],2) if m_data['Clicks']!=0 else 0,axis=1)
    new_df.loc[:, 'Max Bid'] = new_df.loc[:, 'Max Bid'].apply(change_digital.num_del)
    new_df.loc[:,'ratio']=new_df.apply(lambda m_data:round(m_data['Max Bid']/m_data['CPC'],2) if m_data['CPC']!=0 else 0,axis=1)
    new_df = new_df.loc[(new_df['ratio']!=0)&(new_df['Max Bid']>new_df['CPC'])]

    # 分组并获取数值最多的个数
    all_cam_ratio={}
    for cam,group in new_df.groupby('Campaign'):
        all_ratio_num=group.shape[0]
        all_cam_ratio[simplify_name(cam)] = group.loc[:,'ratio'].value_counts().index[0]
    #print all_cam_ratio
    return all_cam_ratio

def advise(campaign):
    pass
    stand_content = "6.建议"


def reback_no_data(campaign,shop_station_str):
    name_abbr = member_judge_mac.member_info_get_name()
    # name_str = name_dict.name_dict.get(name_abbr, 0)
    name_str = name_dict.name_dict_func()
    if name_str:
        name = name_str
    else:
        name = name_abbr
    # name = name_dict.name_dict[member_judge_mac.member_info_get_name()]
    if not campaign.empty:
        insert_acos_into_database.insert_into_only_station_info(
            [shop_station_str.lower(), name, '0', '0', '0', '0', '0'])
    else:
        insert_acos_into_database.insert_into_only_station_info(
            [shop_station_str.lower(), name, '', '', '', '0', ''])


# 将个站点数据以txt的格式,内容是json
def station_info_json(station_info, station):
    station_json = json.dumps(station_info)
    station_dir = station.upper().replace('-','_')
    json_info_dir = r"C:\KMSOFT\Config" + "/" + station_dir + "/station_info.txt"
    with open(json_info_dir) as fp:
        fp.write(station_json)


def operator_time_reback(campaign,shop_station_str):
    name_abbr = member_judge_mac.member_info_get_name()
    name_str = name_dict.name_dict_func()
    if name_str:
        name = name_str
    else:
        name = name_abbr
    if not campaign.empty:
        insert_acos_into_database.insert_into_only_station_info(
            [shop_station_str.lower(), name, '0', '0', '0', '0', '0'])
    else:
        insert_acos_into_database.insert_into_only_station_info(
            [shop_station_str.lower(), name, '', '', '', '0', ''])


# 加入是否生成了上传文件的判断
def main_func(file_path):
    # 判断该站点是否已经被接手
    # user_name = name_dict.name_dict[member_judge_mac.member_info_get_name()]
    user_name = name_dict.name_dict_func()
    now_station = (os.path.basename(file_path)).lower()
    station_ad_manger = "SELECT ad_manger FROM only_station_info WHERE station='%s'" % now_station
    station_row_df = sql_write_read.read_table(station_ad_manger)
    if not station_row_df.empty:
        db_admange = list(station_row_df['ad_manger'])[0]
        if db_admange != user_name:
            # 窗口弹出
            top_f = tk.Tk()
            top_f.title('站点提示')
            top_f.resizable(False, False)
            top_f.attributes("-toolwindow", 1)
            # 得到屏幕高度
            sw = top_f.winfo_screenwidth()
            sh = top_f.winfo_screenheight()
            top_f_w = 350
            top_f_h = 25
            top_f.geometry("%dx%d+%d+%d" % (top_f_w, top_f_h, (sw - top_f_w)/2, (sh-top_f_h)/2 - 200))
            content = "该站点已经被【"+db_admange+"】接手，请联系【C豆】了解具体信息"
            content_entry = tk.StringVar()
            content_entry.set(content)
            tk.Entry(top_f, width=55, textvariable = content_entry).grid(row = 1, column = 0)
            top_f.mainloop()
    # 开始执行统计程序
    # out_path = file_path + "/店铺总结报告.docx"
    #if os.path.exists(out_path):
        #win32api.ShellExecute(0, 'open',out_path, '', '', 1)
        #return
    if not re.search('[A-Za-z]+',str(file_path)):return
    initdata = pd.DataFrame()
    cam_path = unicode("D:/待处理")
    br_statistic = pd.DataFrame()
    file_num = 0
    for child in os.listdir(file_path):
        child_path=os.path.join(file_path,child)
        if os.path.isfile(child_path):
            file_num += 1
            if re.search(r'bulksheet',child):
                cam_path = child_path
                # initdata = pd.read_excel(cam_path, sheet_name='Sponsored Products Campaigns')
                initdata = read_campaign.read_campaign(cam_path)
                initdata.fillna(' ', inplace=True)
            elif re.search(r'Business',child):
                br_path = child_path
                br_statistic = pd.read_csv(child_path)
                # try:br_statistic=pd.read_csv(child_path)
                # except:
                #     br_statistic = pd.read_csv(child_path, quoting=csv.QUOTE_NONE)
                #     br_statistic.columns = [one_col.strip('"') for one_col in br_statistic.columns]
                #     def text_strip(m_data):
                #         try:return m_data.strip('"')
                #         except:return m_data
                #     br_statistic = br_statistic.applymap(text_strip)
                #     row_num, columns_num = br_statistic.shape
                #     br_statistic['index'] = [i for i in range(row_num)]
                #     br_statistic.set_index('index', drop=True, inplace=True)
                #     br_statistic.fillna(0, inplace=True)
    # 文件个数判断
    if file_num < 5:
        print "文件个数不够"
        return
    # 获取站点信息
    shop_station = get_shop_station_from_folder.get_store_station_folder(file_path)
    shop = shop_station[0]
    station_abbr = shop_station[1]
    shop_station_str = shop + '_' + station_abbr
    out_path = file_path + "/" + shop_station_str + u"店铺总结报告.docx"
    # # initdata = initdata.applymap(lambda m_data:m_data.strip())

    # 比较文件创建时间和当前时间 **需优化：创建时间和修改时间都要进行比对
    now_time = int(datetime.datetime.now().strftime('%Y%m%d'))
    creat_time = int((file_time_comp.file_time_get(cam_path)).replace('-', ''))
    br_creat_time = int((file_time_comp.file_time_get(br_path)).replace('-', ''))
    # print now_time,creat_time
    time_split = now_time - creat_time
    br_time_split = now_time - br_creat_time
    if time_split != 0 or br_time_split!=0:
        print "文件不是今天的"
        return
    # 若br或cam为空则不进行处理
    if br_statistic.empty or initdata.empty:
        reback_no_data(initdata, shop_station_str)
        print "cmapaign或br为空"
        return

    # 若满足以上要求直接将操作时间数据回传
    operator_time_reback(initdata, shop_station_str)

    # 统一campaign列名
    initdata = init_campaign.init_campaign(initdata, station_abbr, file_path)
    cam_df_ctr = initdata.copy()

    # 开始处理数据,用新的数据覆盖之前的操作时间数据
    try:
        abstact = acos_judje(initdata,br_statistic,station_abbr,shop_station,shop_station_str,cam_path)
        ad_num = ad_num_statistic(initdata)
        img_path = ad_statistic(initdata,shop_station_str)
        cpc = campaign_cpc(initdata,station_abbr,abstact[-3])
        bid_cpc = campaign_cpc_bid(initdata)

        # try:
        analysis_data=[shop_station_str]+map(json_str,[abstact,ad_num,img_path[1][0],img_path[1][1],
                                    img_path[2][0],img_path[2][1],[list(cpc.loc[one_index]) for one_index in cpc.index],bid_cpc])
        insert_acos_into_database.insert_analysis_station_info(analysis_data)
        # except:pass
    except Exception as err:
        print err
        print shop_station_str+"分析数据无法回传"
        write_to_reocrd.write_error_into_record(shop_station_str+"分析数据无法回传", str(err))
        error_folder_postback.run_error_postback(file_path)
        # 构造新的空的数据供使用
        abstact = []
        cpc = pd.DataFrame()
        #
        # reback_no_data(initdata, shop_station_str)

    # 生成ao表的订单曲线图
    try:
        curve_img = curve_generator.Business_curve()
        curve_img.order_peak_curve(file_path)
        error_folder_postback.reback_order_img(file_path)
    except:pass

    # 生成并回传ST
    st_analysis = multiprocessing.Process(target=st_generator_sheet, args=(file_path, cam_df_ctr))
    st_analysis.start()
    # ctr_auto_df = excavate_auto_low_ctr(cam_df_ctr)
    # return_st_info(file_path, cam_df_ctr, ctr_auto_df)

    # 弹出统计窗口
    if not initdata.empty and len(abstact)!=0 and not cpc.empty:
        message_notice.generator_station_show(shop_station_str,station_abbr,cpc,abstact)


# 生成ST报表的文件
def st_generator_sheet(file_path, cam_df_ctr):
    try:
        ctr_auto_df = excavate_auto_low_ctr(cam_df_ctr)
        return_st_info(file_path, cam_df_ctr, ctr_auto_df)
    except Exception as err:
        print "ST报告生成失败"
        write_to_reocrd.write_error_into_record('ST报告生成失败', str(err))


# 尝试将分析数据回传到数据库
def json_str(kind_data):
    return json.dumps(kind_data,ensure_ascii=False)

def return_st_info(file_path, cam_df, ctr_auto_df):
    # 分析ST数据，并回传
    st_info_get.st_get_info(file_path, cam_df, ctr_auto_df)
    time.sleep(1)
    for one_dir in os.listdir(file_path):
        child_path = os.path.join(file_path, one_dir)
        if os.path.isfile(child_path) and re.search(u'ST报表', one_dir):
            poster_large_file.upload_file(child_path, one_dir)
            try:
                poster_large_file.upload_file_ebay(child_path, one_dir)
            except:pass
        elif os.path.isfile(child_path) and re.search(u'ST出单数据回传', one_dir):
            poster_large_file.upload_st_kw_file(child_path, one_dir)


# 整理点击率特别低于平均值的自动广告
def excavate_auto_low_ctr(cam_df):
    now_df = cam_df.copy()
    # 增加广告组类型
    for one_cam, cam_group in now_df.groupby('Campaign'):
        now_df.loc[cam_group.index, 'type'] = cam_group.loc[cam_group.index[0], 'Campaign Targeting Type']
    now_df = now_df.loc[(now_df['Record Type']=='Ad')&
                        (now_df['type']=='Auto')&
                        (now_df['Campaign Status']=='enabled')&
                        (now_df['Ad Group Status']=='enabled')&
                        (now_df['Status']=='enabled')&
                        (now_df['Impressions']>1000),
                        ['SKU', 'Impressions', 'Clicks', 'Spend', 'Orders','Sales']]
    if now_df.empty:
        print u"曝光大于1000的没有，无法生成点击率表"
        return pd.DataFrame()
    now_df.loc[:, 'Clicks'] = now_df.loc[:, 'Clicks'].astype(float)
    now_df = now_df.groupby('SKU').sum()
    now_df.reset_index(drop=False, inplace=True)
    # now_df.loc[:, 'CTR'] = now_df.apply(
    #     lambda m_data: m_data['Clicks'] / m_data['Impressions'] if m_data['Impressions']!=0 else 0, axis=1)
    now_df.loc[:, 'CTR'] = (now_df.loc[:, 'Clicks']/ now_df.loc[:, 'Impressions']).round(4)
    mean_ctr = round(now_df['CTR'].mean(),4)
    now_df = now_df.loc[now_df['CTR']<mean_ctr]
    # 如果没有低于平均点击率的则返回空白的now_df
    if now_df.empty:return now_df
    now_df.loc[:, 'ACoS'] = now_df.apply(
        lambda m_data: m_data['Spend']/m_data['Sales'] if m_data['Sales'] != 0 else 0,axis=1)
    now_df = now_df.sort_values(by='CTR', axis=0 ,ascending=True)

    now_df.rename(columns={'CTR':'CTR'+'-平均'+str(mean_ctr)}, inplace=True)
    # 将CTR和ACoS转化为百分数，暂时不转

    return now_df


# 整理广告报表中关键词出现数量最多的组成一句话
def excavate_cam_many_kw(cam_df):
    now_df = cam_df.copy()

    # 增加广告组类型
    for one_cam, cam_group in now_df.groupby('Campaign'):
        now_df.loc[cam_group.index, 'type'] = cam_group.loc[cam_group.index[0], 'Campaign Targeting Type']

    # 取出所有的关键词的SKU
    all_manual_sku = now_df.loc[(now_df['type'] == 'Manual')&
                                (now_df['Record Type'] == 'Ad'), ['Campaign', 'Ad Group', 'SKU']].copy()

    now_df = now_df.loc[((now_df['Match Type'] == 'broad')|(now_df['Match Type'] == 'exact')) &
                        (now_df['Campaign Status'] == 'enabled') &
                        (now_df['Ad Group Status'] == 'enabled') &
                        (now_df['Status'] == 'enabled'),
                        ['Campaign', 'Ad Group', 'Keyword', 'Orders']]
    now_df = pd.merge(now_df, all_manual_sku, on=['Campaign', 'Ad Group'], how='left')

    # 汇总同一SKU下的词数，获取前200位的数据
    # now_df.to_excel('123.xlsx')


# 整理广告报表中关st键词出现数量最多的组成一句话
def excavate_st_many_kw(cam_df, st_df):
    now_df = cam_df.copy()

    # 增加广告组类型
    for one_cam, cam_group in now_df.groupby('Campaign'):
        now_df.loc[cam_group.index, 'type'] = cam_group.loc[cam_group.index[0], 'Campaign Targeting Type']

    # 取出所有的关键词的SKU
    all_manual_sku = now_df.loc[(now_df['Record Type'] == 'Ad'),
                                ['Campaign', 'Ad Group', 'SKU']].copy()
    all_manual_sku.rename(columns={'Campaign':'Campaign Name', 'Ad Group':'Ad Group Name'}, inplace=True)

    st_df = st_df.loc[(st_df['Clicks'] > 0)&
                      (st_df['7 Day Total Orders (#)'] > 0)&
                      (~st_df['Customer Search Term'].str.contains('b0')),
                      ['Campaign Name', 'Ad Group Name', 'Customer Search Term', '7 Day Total Orders (#)']]
    now_df = pd.merge(st_df, all_manual_sku, on=['Campaign Name', 'Ad Group Name'], how='left')

    if now_df.empty:
        print u"关键词表为空，无法生成多行高频一句话"
        return pd.DataFrame()
    sku_order = now_df.loc[:, ['SKU', '7 Day Total Orders (#)']].copy()
    sku_order_sum = sku_order.groupby('SKU').sum()
    sku_order_sum.reset_index(drop=False, inplace=True)

    # 汇总同一SKU下的词数，获取前200位的数据
    now_df.loc[:, 'word'] = now_df.apply(
        lambda m_data: (m_data['Customer Search Term']+' ')*m_data['7 Day Total Orders (#)'],axis=1)
    now_df = now_df[['SKU', 'Customer Search Term']]
    now_df.set_index('SKU', inplace=True)
    split_df = now_df['Customer Search Term'].str.split(' ', expand=True)
    split_df.reset_index(drop=False, inplace=True)
    # print split_df.columns

    total_sku_str = {}
    for one_sku,sku_group in split_df.groupby('SKU'):
        count_0_list = []
        for one_col in sku_group.columns:
            if one_col != "SKU" :
                series_count = sku_group[one_col].value_counts()
                count_0_list.append(series_count)
        count_df = pd.DataFrame(count_0_list)#,index=[one_sku]*len(count_0_list))
        # 清除无效的词语
        count_df.columns = [strip_str(one_col).lower() for one_col in count_df.columns]
        for one_col in set(count_df.columns):
            if one_col in ['&', '', ' ', 'for', 'on', 'if', 'of', 's']:
                del count_df[one_col]
            try:num = float(one_col);del count_df[one_col]
            except:pass

        count_df.fillna(0, inplace=True)
        count_series = count_df.sum()
        count_df = pd.DataFrame(count_series.values, index=count_series.index, columns=['SKU'])

        count_df = count_df.sort_values(by='SKU', axis=0, ascending=False).head(200)

        if len(list(count_df.index)) == 1:
            sku_str = list(count_df.index)[0][0:250].split(' ')
        elif len(list(count_df.index)) > 1:
            sku_str = reduce(lambda m_data, n_data: m_data+' '+n_data, list(count_df.index))[0:250].split(' ')
        else:
            sku_str = ""

        if sku_str:
            now_real_str = sku_str[0:(len(sku_str)-1)]
            if len(now_real_str)>1:
                sku_str_abbr = reduce(lambda m_data, n_data: m_data+' '+n_data, sku_str[0:(len(sku_str)-1)])
            elif len(now_real_str)==1:
                sku_str_abbr = now_real_str[0]
            else:sku_str_abbr = ""
            total_sku_str[one_sku] = sku_str_abbr

    total_df = pd.DataFrame(total_sku_str.values(), columns=['Search Term'])
    total_df.insert(0, 'SKU', total_sku_str.keys())
    total_df = pd.merge(total_df, sku_order_sum, on='SKU', how='left')
    # del total_df['SKU']
    total_df = total_df.sort_values(by='7 Day Total Orders (#)', axis=0, ascending=False)
    total_df.rename(columns={'7 Day Total Orders (#)':'近期广告出单量'}, inplace=True)

    return total_df

# 整理广告报表中不出单关st键词出现数量最多的组成一句话，输入为广告报表df和搜索词df,输出为一个df。
# 另外需要将这个df保存为ST 报表添加一个表，表名叫'listing屏蔽不转化词'.
def excavate_no_order_st_many_kw(cam_df, st_df):
    now_df = cam_df.copy()

    # 增加广告组类型
    for one_cam, cam_group in now_df.groupby('Campaign'):
        now_df.loc[cam_group.index, 'type'] = cam_group.loc[cam_group.index[0], 'Campaign Targeting Type']

    # 取出所有的关键词的SKU
    all_manual_sku = now_df.loc[(now_df['Record Type'] == 'Ad'),
                                ['Campaign', 'Ad Group', 'SKU']].copy()
    all_manual_sku.rename(columns={'Campaign':'Campaign Name', 'Ad Group':'Ad Group Name'}, inplace=True)

    st_df = st_df.loc[(st_df['Clicks'] > 0)&
                      (st_df['7 Day Total Units (#)'] == 0)&
                      (~st_df['Customer Search Term'].str.contains('b0')),
                      ['Campaign Name', 'Ad Group Name', 'Customer Search Term', 'Clicks']]
    now_df = pd.merge(st_df, all_manual_sku, on=['Campaign Name', 'Ad Group Name'], how='left')

    if now_df.empty:
        print u"关键词表为空，无法生成多行高频一句话"
        return pd.DataFrame()
    sku_click = now_df.loc[:, ['SKU', 'Clicks']].copy()
    sku_click_sum = sku_click.groupby('SKU').sum()
    sku_click_sum.reset_index(drop=False, inplace=True)

    # 汇总同一SKU下的词数，获取前200位的数据
    now_df.loc[:, 'word'] = now_df.apply(
        lambda m_data: (m_data['Customer Search Term']+' ')*m_data['Clicks'],axis=1)
    now_df = now_df[['SKU', 'Customer Search Term']]
    now_df.set_index('SKU', inplace=True)
    split_df = now_df['Customer Search Term'].str.split(' ', expand=True)
    split_df.reset_index(drop=False, inplace=True)
    # print split_df.columns

    total_sku_str = {}
    for one_sku,sku_group in split_df.groupby('SKU'):
        count_0_list = []
        for one_col in sku_group.columns:
            if one_col != "SKU" :
                series_count = sku_group[one_col].value_counts()
                count_0_list.append(series_count)
        count_df = pd.DataFrame(count_0_list)#,index=[one_sku]*len(count_0_list))
        # 清除无效的词语
        count_df.columns = [strip_str(one_col).lower() for one_col in count_df.columns]
        for one_col in set(count_df.columns):
            if one_col in ['&', '', ' ', 'for', 'on', 'if', 'of', 's']:
                del count_df[one_col]
            try:num = float(one_col);del count_df[one_col]
            except:pass

        count_df.fillna(0, inplace=True)
        count_series = count_df.sum()
        count_df = pd.DataFrame(count_series.values, index=count_series.index, columns=['SKU'])

        count_df = count_df.sort_values(by='SKU', axis=0, ascending=False).head(200)

        if len(list(count_df.index)) == 1:
            sku_str = list(count_df.index)[0][0:250].split(' ')
        elif len(list(count_df.index)) > 1:
            sku_str = reduce(lambda m_data, n_data: m_data+' '+n_data, list(count_df.index))[0:250].split(' ')
        else:
            sku_str = ""

        if sku_str:
            now_real_str = sku_str[0:(len(sku_str)-1)]
            if len(now_real_str)>1:
                sku_str_abbr = reduce(lambda m_data, n_data: m_data+' '+n_data, sku_str[0:(len(sku_str)-1)])
            elif len(now_real_str)==1:
                sku_str_abbr = now_real_str[0]
            else:sku_str_abbr = ""
            total_sku_str[one_sku] = sku_str_abbr

    total_df = pd.DataFrame(total_sku_str.values(), columns=['Search Term'])
    total_df.insert(0, 'SKU', total_sku_str.keys())
    total_df = pd.merge(total_df, sku_click_sum, on='SKU', how='left')
    # del total_df['SKU']
    total_df = total_df.sort_values(by='Clicks', axis=0, ascending=False)
    total_df.rename(columns={'Clicks':'近期广告点击量'}, inplace=True)

    return total_df

def strip_str(col_str):
    for need_strip_str in ['(', ')','"', "'", ".","'"]:
        col_str = col_str.strip().strip(need_strip_str)
    return col_str


def search_num(m_data):
    if re.search('[0-9]', m_data):
        return float(m_data)
    else:
        return 0

def search_get_num(m_data):
    if re.search('[0-9]', m_data):
        return float(re.search('[0-9\.]+', m_data).group())/100
    else:
        return 0

changerate= {'US':1, 'CA':0.7459, 'UK':1.3015, 'DE':1.1241, 'IT':1.1241,
             'ES':1.1241, 'JP':0.008997, 'FR':1.1241, 'MX':0.0513, 'IN':0.01418}

# 获取每个的月各站点数据
def month_station_generator():
    now_month = int(datetime.datetime.now().strftime('%m'))
    month_sql = "SELECT 月份,站点,广告接手人,广告花费,广告带来的销售额,站点总销售额,广告直接带来的利润,广告Click," \
                "广告流量占店铺总流量比例 FROM station_statistic" \
                " WHERE 月份='%s' and 是否计算在内 IN ('算', '是')" % str(now_month-1)
    month_df = sql_write_read.read_table(month_sql)
    month_df.fillna('', inplace=True)
    month_df.loc[:, '数量'] = 1
    month_df = month_df.applymap(lambda m_data: (str(m_data).strip().strip('%')).replace(',', '').replace('，', ''))
    # month_df = month_df.applymap(lambda m_data: float(m_data) if re.search('[0-9]', m_data) else 0)
    month_df.loc[:, u'广告花费'] = month_df.loc[:, u'广告花费'].apply(search_num)
    month_df.loc[:, u'广告带来的销售额'] = month_df.loc[:, u'广告带来的销售额'].apply(search_num)
    month_df.loc[:, u'站点总销售额'] = month_df.loc[:, u'站点总销售额'].apply(search_num)
    month_df.loc[:, u'广告直接带来的利润'] = month_df.loc[:, u'广告直接带来的利润'].apply(search_num)
    month_df.loc[:, u'广告Click'] = month_df.loc[:, u'广告Click'].apply(search_num)
    month_df.loc[:, u'广告流量占店铺总流量比例'] = month_df.loc[:, u'广告流量占店铺总流量比例'].apply(search_get_num)
    # month_df.loc[:, u'广告Click'] = month_df.apply(
    #     lambda m_data: int(str(m_data[u'广告Click']).strip('0')) if m_data[u'广告流量占店铺总流量比例']>=1 else m_data[u'广告Click'],axis=1)
    month_df.loc[:, '数量'] = month_df.loc[:, '数量'].astype(int)

    # 计算月的站点数据
    month_df_2 = month_df.loc[month_df[u'广告接手人'] != '']
    del month_df_2[u'广告接手人']
    month_df_2.loc[:, u'站点'] = month_df_2.loc[:, u'站点'].str.upper()
    sum_df_sta = month_df_2.groupby([u'月份', u'站点']).sum()
    sum_df_sta.reset_index(drop=False, inplace=True)
    sum_df_sta = sum_df_sta.sort_values(by=u'广告带来的销售额', axis=0, ascending=False)

    sum_df_sta.loc[:, 'CPC美金'] = sum_df_sta.apply(
        lambda m_data: round(m_data[u'广告花费'] / m_data[u'广告Click'], 4) if m_data[u'广告Click']!=0 else 0, axis=1)
    sum_df_sta.loc[:, 'CPC本币'] = sum_df_sta.apply(
        lambda m_data: round(m_data['CPC美金'] / changerate[m_data[u'站点']], 2), axis=1)
    sum_df_sta.loc[:, 'CPC美金'] = sum_df_sta.apply(
        lambda m_data: round(m_data[u'广告花费'] / m_data[u'广告Click'], 2)  if m_data[u'广告Click']!=0 else 0, axis=1)
    sum_df_sta.loc[:, 'ACoS'] = sum_df_sta.apply(
        lambda m_data: round(m_data[u'广告花费'] / m_data[u'广告带来的销售额'] , 4) , axis=1)
    sum_df_sta.loc[:, '销售占比'] = sum_df_sta.apply(
        lambda m_data: round(m_data[u'广告带来的销售额'] / m_data[u'站点总销售额'], 4), axis=1)
    sum_df_sta.loc[:, '花费占比'] = sum_df_sta.apply(
        lambda m_data: round(m_data[u'广告花费'] / m_data[u'站点总销售额'] , 4) , axis=1)

    sql_write_read.to_table_only(sum_df_sta, 'month_acos')


# 获取每个的月各站点数据
def month_user_generator():
    now_month = int(datetime.datetime.now().strftime('%m'))
    month_sql = "SELECT 月份,站点,广告接手人,广告花费,广告带来的销售额,站点总销售额,广告直接带来的利润,广告Click," \
                "广告流量占店铺总流量比例 FROM station_statistic" \
                " WHERE 月份='%s' and 是否计算在内 IN ('算', '是')" % str(now_month-1)
    month_df = sql_write_read.read_table(month_sql)
    month_df.fillna('', inplace=True)
    month_df.loc[:, '数量'] = 1
    month_df = month_df.applymap(lambda m_data: (str(m_data).strip().strip('%')).replace(',', '').replace('，', ''))
    # month_df = month_df.applymap(lambda m_data: float(m_data) if re.search('[0-9]', m_data) else 0)
    month_df.loc[:, u'广告花费'] = month_df.loc[:, u'广告花费'].apply(search_num)
    month_df.loc[:, u'广告带来的销售额'] = month_df.loc[:, u'广告带来的销售额'].apply(search_num)
    month_df.loc[:, u'站点总销售额'] = month_df.loc[:, u'站点总销售额'].apply(search_num)
    month_df.loc[:, u'广告直接带来的利润'] = month_df.loc[:, u'广告直接带来的利润'].apply(search_num)
    month_df.loc[:, u'广告Click'] = month_df.loc[:, u'广告Click'].apply(search_num)
    month_df.loc[:, u'广告流量占店铺总流量比例'] = month_df.loc[:, u'广告流量占店铺总流量比例'].apply(search_get_num)
    # month_df.loc[:, u'广告Click'] = month_df.apply(
    #     lambda m_data: int(str(m_data[u'广告Click']).strip('0')) if m_data[u'广告流量占店铺总流量比例']>=1 else m_data[u'广告Click'],axis=1)
    month_df.loc[:, '数量'] = month_df.loc[:, '数量'].astype(int)

    # 计算月的站点数据
    month_df_2 = month_df.loc[month_df[u'广告接手人'] != '']
    sum_df_sta = month_df_2.groupby([u'月份', u'广告接手人']).sum()
    sum_df_sta.reset_index(drop=False, inplace=True)
    sum_df_sta = sum_df_sta.sort_values(by=u'广告带来的销售额', axis=0, ascending=False)

    # sum_df_sta.loc[:, 'CPC美金'] = sum_df_sta.apply(
    #     lambda m_data: round(m_data[u'广告花费'] / m_data[u'广告Click'], 4), axis=1)
    # sum_df_sta.loc[:, 'CPC本币'] = sum_df_sta.apply(
    #     lambda m_data: round(m_data['CPC美金'] / changerate[m_data[u'站点']], 2), axis=1)
    sum_df_sta.loc[:, 'CPC美金'] = sum_df_sta.apply(
        lambda m_data: round(m_data[u'广告花费'] / m_data[u'广告Click'], 2) if m_data[u'广告Click']!=0 else 0, axis=1)
    sum_df_sta.loc[:, 'ACoS'] = sum_df_sta.apply(
        lambda m_data: str(round(m_data[u'广告花费'] / m_data[u'广告带来的销售额'] , 4)*100)+'%' , axis=1)
    sum_df_sta.loc[:, '销售占比'] = sum_df_sta.apply(
        lambda m_data: str(round(m_data[u'广告带来的销售额'] / m_data[u'站点总销售额'], 4)*100)+'%', axis=1)
    sum_df_sta.loc[:, '花费占比'] = sum_df_sta.apply(
        lambda m_data: str(round(m_data[u'广告花费'] / m_data[u'站点总销售额'] , 4)*100)+'%' , axis=1)

    sql_write_read.to_table_only(sum_df_sta, 'month_user')



if __name__=="__main__":
    cam_path1 = unicode(r"D:\待处理\AMOQ\DIYEENI_DE")
    st_path1 = unicode(r"D:\待处理\ZJCHAO\ZJCHAO_US\Sponsored Products Search term report.xlsx")
    root = ""
    main_func(cam_path1)
    # return_st_info(cam_path1)
    # month_user_generator()
    # month_station_generator()
    # cam_df1 = init_campaign.init_campaign(pd.read_excel(cam_path1), 'US')
    # st_df1 = pd.read_excel(st_path1)
    # excavate_st_many_kw(cam_df1, st_df1)
    # excavate_cam_many_kw(cam_df1)
    # excavate_auto_low_ctr(cam_df1)







    # ACOS=CPC/(CR*PRICE)
    # 1.ACOS高，CPC高--CPC高导致，需降低CPC：
    # 降低新增广告的目标acos；调价后，整体乘以降价系数（0.7-0.9）；降低广告大组预算（近两天大组广告花费）；设置竞价上限（平均cpc4倍）
    # 2.ACOS高，CPC低--广告CR低导致，提升CR或CPC偏高，可适当降低CPC：
    # 做重复广告暂停；做精否；否定asin；做差广告暂停；（可适当降低CPC，参照第一点）
    # 3.ACOS低，CPC高--CR高或客单价高：（可适当降低CPC），参照第一点

    # 广告销售额占比不够，ACOS低--广告没有完全覆盖：全面新增（可以做active_listing,或者做AO时，客单价限制下调）
    # 广告销售额占比不够，ACOS高--广告没有完全覆盖：全面新增（可以做active_listing,或者做AO时，客单价限制下调）
    # 广告销售额占比够，ACOS高--广告没有完全覆盖：全面新增（可以做active_listing,或者做AO时，客单价限制下调）



