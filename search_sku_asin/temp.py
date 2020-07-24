import time
import pandas as pd
import pymysql
import numpy as np
import os,requests
from retry import retry

@retry(tries=3,delay=2)
def okk():
    i = 0
    services_ip_name = 'http://120.168.9.167/'
    request_file = requests.get(services_ip_name)
    i+=1
    print(i)


if __name__ == "__main__":
    a = np.arange(1,10)
    print(a)




