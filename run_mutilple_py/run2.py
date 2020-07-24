# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/4/23 18:01
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
import re, os, pymysql, time, redis
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

PROCESS_POOL = ProcessPoolExecutor(4)
sale_exchange_rate = {'CA': 0.7519, 'DE': 1.0981, 'FR': 1.0981, 'IT': 1.0981, 'SP': 1.0981, 'JP': 0.009302,
                      'UK': 1.2445, 'MX': 0.05147, 'IN': 0.01412, 'US': 1, 'ES': 1.0981, 'AU': 0.6766}
redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, password='chy910624', db=3, decode_responses=True)
red = redis.StrictRedis(connection_pool=redis_pool)


def print_even():
    for i in range(20):
        if i % 2 != 0:
            time.sleep(1)
            datetime_now = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
            red.set(f'{100 + i}_{datetime_now}', i)


if __name__ == "__main__":
    print_even()
    print('finish!')
