# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/4/23 18:04
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""
import redis, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

Thread_Pool = ThreadPoolExecutor(2)
redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, password='chy910624', db=1, decode_responses=True)
red = redis.StrictRedis(connection_pool=redis_pool)


def run_py():
    py_name = red.lpop('run_py')
    if py_name is not None:
        print(py_name)
        with open(py_name, 'r') as f:
            exec(f.read())


# 多线程执行读写频繁任务
def process_read_file():
    run_pys = ['D:/AD-Helper1/ad_helper/recommend/run_mutilple_py/run1.py', 'run2.py']
    red.rpush('run_py', *run_pys)
    while 1:
        all_task = []
        for one_page in range(2):
            all_task.append(Thread_Pool.submit(run_py))
        for future in as_completed(all_task):
            future.result()
        if red.llen('run_py') == 0:
            break


if __name__ == "__main__":
    process_read_file()
