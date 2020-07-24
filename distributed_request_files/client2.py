# -*- coding: utf-8 -*-
"""
Proj: recommend
Created on:   2020/1/17 11:35
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""


from socket import *
import pandas as pd
import struct
import json
import time
import os
import shutil

client_socket = socket(AF_INET, SOCK_STREAM)
server_address = '192.168.30.226'  # 服务器的ip地址
buffsize = 1024
server_port = 12378  # 服务器的端口号
# 与服务器建立连接
client_socket.connect((server_address, server_port))
# 接收信息
while True:
    head_struct = client_socket.recv(4)  # 接收报头的长度
    if head_struct:
        print('已连接服务端,等待接收数据')
    head_len = struct.unpack('i', head_struct)[0]  # 解析出报头的字符串大小
    data = client_socket.recv(head_len)  # 接收长度为head_len的报头内容的信息 (包含文件大小,文件名的内容)

    head_dir = json.loads(data.decode('utf-8'))
    filesize_b = head_dir['filesize_bytes']
    filename = head_dir['filename']
    file_dirname = os.path.dirname(filename)
    file_basename = os.path.basename(filename)
    give_dirname = 'D:/trans_file'
    # 判断母路径是否存在
    if os.path.exists(file_dirname):
        # 保存的路径
        new_filename = file_dirname + "/new1_" + file_basename
        print(new_filename)
    elif os.path.exists(give_dirname):
        new_filename = give_dirname + "/new1_" + file_basename
    else:
        os.makedirs(give_dirname)
        new_filename = give_dirname + "/new1_" + file_basename
    if os.path.exists(new_filename):
        os.remove(new_filename)

    #   接受文件内容
    recv_len = 0
    recv_mesg = b''
    old = time.time()
    f = open(new_filename, 'wb')
    while recv_len < filesize_b:
        if filesize_b - recv_len > buffsize:

            recv_mesg = client_socket.recv(buffsize)
            f.write(recv_mesg)
            recv_len += len(recv_mesg)
        else:
            recv_mesg = client_socket.recv(filesize_b - recv_len)
            recv_len += len(recv_mesg)
            f.write(recv_mesg)

    print(recv_len, filesize_b)
    now = time.time()
    stamp = int(now - old)
    print('总共用时%ds' % stamp)
    f.close()
    client_socket.shutdown(2)
    client_socket.close()
    break

shutil.unpack_archive(new_filename, 'D:/待处理/Download_temp/')
os.remove(new_filename)




