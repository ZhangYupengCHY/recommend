import pymysql
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
    'marmot', '', 'wuhan.yibai-it.com', 33061, 'team_station', 'utf8'))


def to_table(df):
    # engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('user', '', '192.168.16.106',
    # 'team_station','utf8'))
    con = engine.connect()  # 创建连接
    # 分多个表写入
    rows, columns = df.shape
    # 分成四个表依次写入
    split_rows = rows / 4
    other_rows = rows % 4
    df1, df2, df3, df4 = df.loc[0:split_rows - 1], df.loc[split_rows:2 * split_rows - 1], \
                         df.loc[2 * split_rows:3 * split_rows - 1], df.loc[
                                                                    3 * split_rows:4 * split_rows - 1 + other_rows]
    df1.to_sql(name='order_shop_1', con=con, if_exists='replace', index=False)
    df2.to_sql(name='order_shop_2', con=con, if_exists='replace', index=False)
    df3.to_sql(name='order_shop_3', con=con, if_exists='replace', index=False)
    df4.to_sql(name='order_shop_4', con=con, if_exists='replace', index=False)


def to_table_only(df, db_name):
    # engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('user', '', '192.168.16.106',
    # 'team_station','utf8'))
    con = engine.connect()  # 创建连接
    df.to_sql(name=db_name, con=con, if_exists='fail', index=False)
    con.close()


def to_table_replace(df, db_name):
    con = engine.connect()  # 创建连接
    df.to_sql(name=db_name, con=con, if_exists='replace', index=False)
    con.close()


def to_table_append(df, db_name):
    con = engine.connect()  # 创建连接
    df.to_sql(name=db_name, con=con, if_exists='append', index=False,chunksize=1000000)
    con.close()


# 删除数据库中某些表的数据
def delete_table(table_name, db='team_station', port=33061, ip='wuhan.yibai-it.com',
                 user_name='user', password=''):
    conn = pymysql.connect(host=ip, port=port, user=user_name, password=password, db=db, charset='utf8')
    # 创建游标
    cursor = conn.cursor()
    sql_delete = "DELETE FROM %s" % table_name

    try:
        cursor.execute(sql_delete)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    cursor.close()
    conn.close()


def read_table(sql):
    con = engine.connect()  # 创建连接
    # find_list_new=[str(m_data) for m_data in find_list if m_data!='']
    # sql="select 单据编号,往来单位 from %s where 单据编号 in (%s)"%(table_name,','.join(find_list))
    df_data = pd.read_sql(sql, con)
    con.close()

    return df_data


def read_db_table(sql, db_name):
    engine_db = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format(
        'user', '', '192.168.129.240', db_name, 'utf8'))
    con = engine_db.connect()  # 创建连接
    df_data = pd.read_sql(sql, con)
    con.close()

    return df_data


def read_ad_db_table(sql, db_name):
    engine_db = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format(
        'marmot', '', '192.168.129.240', db_name, 'utf8'))
    con = engine_db.connect()  # 创建连接
    df_data = pd.read_sql(sql, con)
    con.close()

    return df_data


def to_db_table_fail(df, db_name, sheet_name):
    engine_db = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format(
        'user', '', '192.168.129.240', db_name, 'utf8'))
    con = engine_db.connect()  # 创建连接
    df.to_sql(name=sheet_name, con=con, if_exists='fail', index=False)
    con.close()


def read_table_kw(sql):
    engine_kw = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format(
        'user', '', '192.168.129.240', 'sku_ad_history', 'utf8'))
    con = engine_kw.connect()  # 创建连接
    # find_list_new=[str(m_data) for m_data in find_list if m_data!='']
    # sql="select 单据编号,往来单位 from %s where 单据编号 in (%s)"%(table_name,','.join(find_list))
    df_data = pd.read_sql(sql, con)
    con.close()

    return df_data


def read_local_db(sql):
    local_engine = create_engine(
        f"mysql+pymysql://{'marmot'}:{''}@{'192.168.129.240'}/{'team_station'}?charset={'utf8'}")
    local_con = local_engine.connect()  # 创建连接
    df_data = pd.read_sql(sql, local_con)
    local_con.close()

    return df_data


def to_local_table_replace(df, db_name):
    local_engine = create_engine(
        f"mysql+pymysql://{'marmot'}:{''}@{'192.168.129.240'}/{'team_station'}?charset={'utf8'}")
    con = local_engine.connect()  # 创建连接
    df.to_sql(name=db_name, con=con, if_exists='replace', index=False)
    con.close()



if __name__ == '__main__':
    sql = 'select * from yibai_amazon_sku_map limit 100'
    a = read_table(sql)
    print(a)