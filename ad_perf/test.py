#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/22 0022 17:02
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : test.py
from datetime import datetime,timedelta

import pandas as pd
a = 30
print(f'This is a \033[{a}m test \033[0m')
print('This is a \033[8;30;31m test \033[0m')
print('\033[1;33;44m This is a test !\033[0m')

