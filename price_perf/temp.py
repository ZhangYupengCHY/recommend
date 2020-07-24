# -*- coding: utf-8 -*-


"""
Proj: AD-Helper1
Created on:   2019/11/25 16:10
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import re
import pandas as pd
from datetime import datetime



if __name__ == '__main__':
    a = pd.DataFrame([[1,2,3],[2,3,4]],columns=['e','t','r'])
    a.set_index('t',inplace=True)