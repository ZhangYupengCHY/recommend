
"""
对各个国家的汇率进行转换，统一规范为美元.
目前涉及到九个国家（2019/9/23日的汇率）：
加拿大 加币 ca 0.7519美元、（德国、法国、意大利、西班牙） 欧元 1.0981美元、日本 日元 jp 0.009302美元 、墨西哥 墨西哥币 mx 0.05147美元、英国 英镑 uk 1.2445美元 印度 in 0.01412美元
"""
import pandas as pd
import pymysql


def conn_mysql(process_database='camp_report'):
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database=process_database,
        port=3306,
        charset='UTF8')
    return conn


# 将数据库中的数据读取
def load_account_info(table='calc_camp'):
    # 连接数据库
    conn = conn_mysql()
    # 创建游标
    cursor = conn.cursor()
    # 写sql
    sql = """SELECT * FROM {} order by account,site,date""".format(table)
    # 执行sql语句
    cursor.execute(sql)
    all_result = cursor.fetchall()
    all_result = pd.DataFrame([list(j) for j in all_result],
                              columns=['account', 'site', 'date', 'acos', 'cpc', 'cr', 'spend', 'sales', \
                                       'impression', 'clicks', 'add_spend', 'add_sales', 'add_impression', 'add_clicks', \
                                       'add_acos'])
    conn.commit()
    cursor.close()
    conn.close()
    return all_result


def init_parms(df):
    changed_columns = ['cpc', 'spend', 'sales', 'add_spend', 'add_sales']
    # 各个国家换换算成美元的汇率
    nations_exchange_rate = {'ca': 0.7519, 'de': 1.0981, 'fr': 1.0981, 'it': 1.0981, 'sp': 1.0981, 'jp': 0.009302,
                             'uk': 1.2445, 'mx': 0.05147, 'in': 0.01412, 'us': 1}
    df[changed_columns] = df[changed_columns].astype('float')

    all_dataframe = pd.DataFrame(columns=changed_columns)
    for nation in nations_exchange_rate:
        temp = df[changed_columns][df['site'] == nation].applymap(lambda x: x * nations_exchange_rate[nation])
        all_dataframe = pd.concat([all_dataframe, temp], axis=0)
    df[changed_columns] = all_dataframe[changed_columns]
    return df


if __name__ == '__main__':
    data = load_account_info()
    info = init_parms(data)