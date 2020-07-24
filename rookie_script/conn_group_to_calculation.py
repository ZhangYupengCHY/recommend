import paramiko


def generator_station_show(shop_station, station_abbr, acos_df, abstact):
    # station_abbr, ad_acos, ad_cpc, acos_df, ad_per
    month_sql = "SELECT CPC本币,ACoS,销售占比 FROM month_acos WHERE 站点='%s'" % station_abbr
    month_df = sql_write_read.read_table(month_sql)
    # 获取站点月统计数据
    last_station_per = list(month_df.loc[0])[2]
    last_station_per_str = str(round(last_station_per * 100, 1)) + "%"
    month_acos = list(month_df.loc[0])[1]
    acos_str = str(round(month_acos * 100, 1)) + "%"
    month_cpc = list(month_df.loc[0])[0]

    # 从摘要中获取广告销售额
    if abstact:
        ad_sales = abstact[3].split('，')[0].split('：')[1]
        ad_cr = (abstact[3].split('，')[2].split('：')[1]).split('。')[0]
        # 花费占比
        spend_shop = (abstact[4].split('，')[1].split('：')[1]).split('。')[0]
    else:
        ad_sales = "1"
        ad_cr = "暂无"
        spend_shop= "暂无"

    ad_acos_num, ad_cpc, ad_per = abstact[-3], abstact[-2], abstact[-1]

    ad_acos_str = str(round(ad_acos_num * 100, 1)) + "%"
    ad_per_num = float(ad_per.strip('%')) / 100
    result_1, result_2, result_3 = "", "", ""
    if ad_acos_num > month_acos:
        result_1 = "ACoS过高 "
    if ad_per_num < last_station_per:
        result_2 = "占比过低 "
    if ad_acos_num < month_acos and last_station_per < ad_per_num:
        result_3 = "继续保持"

    # 获取站点的上月毛利率
    now_month = int(datetime.datetime.now().strftime('%m'))
    last_month = str(now_month - 1)
    # 上月acos，上月cpc，上月广告转化率，上月广告销售额占比，上月推广占比
    station_sql = "SELECT 广告花费,acos,广告带来的销售额,广告Click,广告转化率,广告花费占总销售额的比例," \
                  "广告直接带来的销售额占站点总销售额的占比," \
                  "站点实际毛利率 FROM station_statistic WHERE 账号站点='%s' and 年月='%s' and 月份='%s'" % \
                  (shop_station.replace('_', '-'), '2019', last_month)
    gross_search_result = get_data_from_database.search(station_sql)
    # 若上月没有，则去上上个月的
    if len(gross_search_result) == 0:
        last_month = str(now_month - 2)
        station_sql = "SELECT 广告花费,acos,广告带来的销售额,广告Click,广告转化率,广告花费占总销售额的比例," \
                      "广告直接带来的销售额占站点总销售额的占比," \
                      "站点实际毛利率 FROM station_statistic WHERE 账号站点='%s' and  年月='%s' and 月份='%s'" % \
                      (shop_station.replace('_', '-'), '2019', last_month)
        gross_search_result = get_data_from_database.search(station_sql)
    # print gross_search_result
    # 若上上个月没有则返回暂无
    if len(gross_search_result) == 0:
        gross_result = "暂无"
        last_spend = "暂无"
        last_acos = "暂无"
        last_sales = "暂无"
        last_cpc = "暂无"
        last_ad_cr = "暂无"
        last_spend_shop = "暂无"
        last_sale_per = "暂无"
    else:
        gross_result = gross_search_result[0][-1]
        last_spend = gross_search_result[0][0]
        last_acos = gross_search_result[0][1]
        last_sales = gross_search_result[0][2]
        last_click = gross_search_result[0][3]
        last_ad_cr = gross_search_result[0][4]
        last_spend_shop = gross_search_result[0][5]
        last_sale_per = gross_search_result[0][6]
        if float(last_click) != 0:
            last_cpc = round(float(last_spend)/float(last_click), 2)
        else:
            last_cpc = 0

    # 取出少卖的大组
    less_df = acos_df.head(3)
    row_name = json.dumps(list(less_df['simplify_name']))
    less_sales = json.dumps(list(less_df['need_sale_campare']))

    # 窗口弹出
    top_f = tk.Tk()
    # top_f = tk.Toplevel(root)
    top_f.title('站点概览')
    top_f.resizable(False, False)
    top_f.attributes("-toolwindow", 1)
    # if screen_size.size_judge(root) == 'right':
    #     screen_size.right_screen_size(root, top_f, 190, 230)
    # else:
    #     screen_size.left_screen_size(root, top_f, 190, 230)
    # top_f.attributes("-alpha", 0.9)  # 窗口透明度70 %
    # 得到屏幕高度
    sw = top_f.winfo_screenwidth()
    sh = top_f.winfo_screenheight()
    top_f_w = 220
    top_f_h = 350
    top_f.geometry("%dx%d+%d+%d" % (top_f_w, top_f_h, sw - top_f_w - 20, 5))

    # tk.Label(top_f, text="站点概览:").grid(row=0, column=0,rowspan=1,columnspan=1)
    #
    # now_acos = tk.StringVar()
    # tk.Label(top_f, text="ACoS:").grid(row=9, column=0)
    # tk.Entry(top_f, textvariable=now_acos).grid(row=9, column=1)

    # 创建文本框
    if ad_acos_num > month_acos and ad_per_num < last_station_per:
        T = tk.Text(top_f, height=27, width=30, fg="red")
    elif ad_acos_num > month_acos or ad_per_num < last_station_per:
        T = tk.Text(top_f, height=27, width=30, fg="#CD5C5C")
    else:
        T = tk.Text(top_f, height=27, width=30, fg="blue")
    T.grid(row=7, column=0, rowspan=6, columnspan=2)
    content = "     " + shop_station + "站点概览\n" \
              "-----------------------------" + "\n" + \
              "广告销售额：" + ad_sales + "\n" + \
              "-----------------------------" + "\n" + \
              "ACoS：" + ad_acos_str + "，上月：" + last_acos + "\n" + \
              "-----------------------------" + "\n" + \
              "占比：" + str(ad_per) + "，上月：" + last_sale_per + "\n" + \
              "-----------------------------" + "\n" + \
              "CPC：" + str(ad_cpc) + "，上月：" + str(last_cpc) + "\n" + \
              "-----------------------------" + "\n" + \
              "广告转化：" + ad_cr + "，上月：" + last_ad_cr + "\n" + \
              "-----------------------------" + "\n" + \
              "花费占比：" + spend_shop + "，上月：" + last_spend_shop + "\n" + \
              "-----------------------------" + "\n" + \
              "少卖倒数三：\n" +  row_name +"\n" + \
              "-----------------------------" + "\n" + \
              "少卖：\n" + less_sales + "\n" + \
              "-----------------------------" + "\n" + \
              "总结：" + result_1 + result_2 + result_3 + "\n" + \
              "-----------------------------" + "\n" + \
              "及时暂停销售自建广告中近一月\n未出单sku"
    T.insert(tk.END, content + '\n')
    T['state'] = 'disabled'

    top_f.mainloop()