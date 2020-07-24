# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 17:07:52 2019

@author: Administrator
"""

import pandas as pd
import numpy as np
import glob
import os
import re
import base64
import openpyxl
import datetime
import pymysql
import pandas as pd
import os
import numpy as np
import datetime
import pymysql
import threading, time
from datetime import datetime
import shutil


# =============================================================================
# # 创建一个数组
# a = pd.DataFrame([['1awert,aasdsad','2a',3,4],['1','4a',4,5],['2','3a',4,5],['1awert,er','2a',4,4],['1awert,eer','2a',3,5],['1awert,ert','2a',5,4],['1awert','2a',4,998],['1awert,ert','2a',5,4]],columns=['e','r','t','y'])
#
#
#
# d = pd.DataFrame(np.arange(100000).reshape((-1,5)))
#
# # 对多线程简单实验 I/O
# =============================================================================


def my_input():
    pls = input("请输入您要查询的事情:")


if __name__ == '__main__':
    print('start...')
    all_thread = []
    one = threading.Thread(target=my_input,args=(,))
