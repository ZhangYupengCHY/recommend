# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/3/9 14:57
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import pandas as pd
import numpy as np
import os,pymysql
import re
import difflib
from datetime import datetime
import re
import shutil


import os
import time
import pandas as pd
import re

a= {'er','RT'}
a = set(map(lambda x:x.lower(),a))
print(a)
