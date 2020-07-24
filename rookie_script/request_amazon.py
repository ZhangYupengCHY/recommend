import requests
import pandas as pd
import datetime
import re
import numpy as np
from bs4 import BeautifulSoup

path_py = r"C:\Users\Administrator\Desktop\amazon\asin.xlsx" #初始化工作路径

header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.118 Safari/537.36'}
#初始化header
asins_own = []
asins_competitor = []
review_list = []
price_list = []
star_list = []
#初始化列表为空


data = pd.read_excel(path_py,sheet_name='Sheet1')
print(data)
data = data['asin']
train_data = np.array(data)#np.ndarray()
alist=train_data.tolist()
# for i in alist:
#     asins_own.append(i[0])
#     print(asins_own)

def crawl(asin):
    html = requests.get('https://www.amazon.com/dp/'+asin, headers=header)
    html.encoding = 'utf-8' #这一行是将编码转为utf-8否则中文会显示乱码。
    soup = BeautifulSoup(html.text, 'html.parser')
    try:
        review = re.sub("\D","",soup.find("span",{'id':"productTitle"}).contents[0]) #通过re.sub...\D过滤非数字内容
        review_list.append(review)
    except:
        review_list.append('0') #若找不到所需结果则储存0
    try:
        price = soup.find("span",{'id':"priceblock_ourprice"}).contents[0]
    except:
        price = "Lost Buy Box"
        price_list.append(price)
    try:
        star = soup.find("span",{'class':"acrCustomerReviewText"}).contents[0][0:3]
        star_list.apend(star)
    except:
        star_list.append('N/A')
    print(review_list)
    print(price_list)
    print(star_list)
    with open( path_py + asin + '_' '.txt' , 'w', encoding='utf-8') as f:
        f.write(html.text)
        f.close()
    return()

for asin in alist:
    crawl(asin)
    print('page for ASIN:' + asin + " downloaded")


today = datetime.date.today()

dataframe = pd.DataFrame({'ASIN':asins_own,'Reviews':review_list,'Price':price_list,'Star':star_list})


dataframe.to_csv(path_py + str(today) + ".csv",index=False,sep=',')

    






















