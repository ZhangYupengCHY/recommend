"""
读取text中的内容，并检查mysql是否存在该数据，如果内容存在则更新到，不存在则添加
"""
import pymysql
import pandas as pd


# 连接数据库 import pymysql
def conn_mysql(process_database):
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database=process_database,
        port=3306,
        charset='UTF8')
    return conn


# 将数据库中的数据读取
def load_account_info(**kwargs):
    # 连接数据库
    conn = conn_mysql(kwargs['database'])
    # 创建游标
    cursor = conn.cursor()
    # 写sql
    sql = """SELECT * FROM {} """.format(kwargs['table'])
    # 执行sql语句
    cursor.execute(sql)
    all_result = cursor.fetchall()
    all_result = [list(j) for j in all_result]
    conn.commit()
    cursor.close()
    conn.close()
    return all_result


# 第二列为account列
def check_update_or_add(one_line,all_lines,**kwargs):
    # 汇总所有的account
    all_accounts = []
    for one_row in all_lines:
        all_accounts.append(one_row[kwargs['account_row']])
    # case 1:新的account_site,将信息添加到数据表中
    if one_line[kwargs['account_row']] not in all_accounts:
        all_lines.append(one_line)
    # case 2: 没有更新数据,不做处理
    elif one_line in all_lines:
        pass
    # case 3: 数据更新
    else:
        index_account = all_accounts.index(one_line[kwargs['account_row']])
        all_lines[index_account] = one_line


def update_mysql(split_by = '\t'):
    f = open(r"C:\Users\Administrator\Desktop\2019-09-27_new_api_request_part_text.txt", "r")
    lines = f.readlines()  # 读取全部内容
    all_account = load_account_info(database='camp_report',table='calc_camp')
    for line in lines:
        line = line.strip('\n').strip('\t')
        line = line.split(split_by)
        check_update_or_add(line,all_account,account_row=1)
    return pd.DataFrame(all_account)


if __name__ == "__main__":
    result_account_change_info = update_mysql()
    print(len(result_account_change_info))
