import redis
import pandas as pd
import os
import re
import base64


# 将excel存储到redis中
def excel_2_redis(path, sheet_name=0):
    r = redis.Redis(host='localhost', port=6379, db=0, password='chy910624', decode_responses=True)
    excel_data = pd.read_excel(path)
    excel_data = excel_data.applymap(lambda x: str(x))
    for column in excel_data.columns:
        for index in excel_data.index:
            key = '{}:{}'.format(column, index)
            value = excel_data.loc[index, column]
            r.set(key, value)


# 得到每个人的文件夹
def get_staff_folder(folder_dir):
    # 得到文件下的次级文件夹(站点负责人的名字)
    folder_list = os.listdir(folder_dir)
    # 人名
    all_name_list = ['张于鹏', '张于鹏2', '张三']
    name_list = [list for list in folder_list if list in all_name_list]
    name_folders = [os.path.join(folder_dir, name) for name in name_list]
    return name_folders


# 获得某个人的所有文件
def get_all_files(name_dirname):
    return [os.path.join(dirpath, file) for dirpath, dirname, filename in os.walk(name_dirname) for file in filename]


# 将每个人的站点信息导入到redis中
def accounts_2_redis(folder_dir):
    name_folders_list = get_staff_folder(folder_dir)
    pass


if __name__ == '__main__':
    excel_2_redis(r'C:\Users\Administrator\Desktop\1091_heijom美国_AmazonSponsoredProductsBulk_2019-10-12.xlsx')

    # a = download_redis()
    # c = trans_redis_2_df(a)
    # c.to_excel('C:/Users/Administrator/Desktop/w123.xlsx', index=False)
