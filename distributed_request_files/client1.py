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
import struct
import json
import time, zipfile
import os
import shutil
import tkinter as tk
import tkinter.messagebox
from datetime import  datetime


def socket_client(file_dirname):
    print("客户端启动...")
    client_socket = socket(AF_INET, SOCK_STREAM)
    server_address = '192.168.129.240'  # 服务器的ip地址
    buffsize = 1024
    server_port = 3553  # 服务器的端口号
    # 与服务器建立连接
    client_socket.connect((server_address, server_port))
    # 接收信息
    client_socket.settimeout(3)
    try:
        head_struct = client_socket.recv(4)  # 接收报头的长度
    except:
        print("没有报表文件.")
        return
    if head_struct:
        print('已连接服务端.')
    head_len = struct.unpack('i', head_struct)[0]  # 解析出报头的字符串大小
    data = client_socket.recv(head_len)  # 接收长度为head_len的报头内容的信息 (包含文件大小,文件名的内容)

    head_dir = json.loads(data.decode('utf-8'))
    filesize_b = head_dir['filesize_bytes']
    filename = head_dir['filename']
    # 压缩文件临时路径
    file_basename = os.path.basename(filename)
    # 判断母路径是否存在
    if os.path.exists(file_dirname):
        shutil.rmtree(file_dirname)
        # 保存的路径
    os.makedirs(file_dirname)
    new_filename = file_dirname + "/znewfile_" + file_basename
    # 如果存在压缩文件，删除点
    if os.path.exists(new_filename):
        os.remove(new_filename)

    #   接受文件内容
    recv_len = 0
    recv_mesg = b''
    old = time.time()
    f = open(new_filename, 'wb')
    print('正在接收数据,请等待..')
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
    print(f'传输站点数据总共用时: {stamp} S')
    print("稍等,等待解压...")
    f.close()
    client_socket.shutdown(2)
    client_socket.close()


def unzip_manger_folder(file_dirname):
    if not os.path.exists(file_dirname):
        root = tk.Tk()
        root.withdraw()
        tkinter.messagebox.showinfo(message=f'没有请求到站点报表.\n')
        return
    zipped_manger_list = os.listdir(file_dirname)
    if not zipped_manger_list:
        return

    def unzip_station(file_dirname: 'zipped_file_dirname', file_basename: 'zipped_file_basename', zip_to_path):
        zip_file_full_path = os.path.join(file_dirname, file_basename)
        if not zipfile.is_zipfile(zip_file_full_path):
            return
        # 排除将某人的压缩包解压成站点压缩包的情况,只更新(先删除后解压)站点压缩包数据
        if file_dirname != zip_to_path:
            if os.path.exists(zip_to_path):
                shutil.rmtree(zip_to_path)
        if not os.path.exists(zip_to_path):
            os.makedirs(zip_to_path)
        zip_file = zipfile.ZipFile(zip_file_full_path, 'r')
        zip_file.extractall(path=zip_to_path)
        zip_file.close()

    # 负责人的全站点压缩包
    zipped_manger_name = zipped_manger_list[0]
    # 首先将负责人的压缩包解压得到站点的解压包
    unzip_station(file_dirname, zipped_manger_name, file_dirname)
    os.remove(os.path.join(file_dirname, zipped_manger_name))
    stations_list = os.listdir(file_dirname)
    zip_to_path = [os.path.join(os.path.dirname(file_dirname), zipped_station_basename[:-7].upper(), zipped_station_basename[:-4].upper())
                   for zipped_station_basename in stations_list]
    # shutil.rmtree(file_dirname)
    if not stations_list:
        return
    [unzip_station(file_dirname, station_zipped_file, zipped_to_path) for station_zipped_file, zipped_to_path in
     zip(stations_list, zip_to_path)]
    shutil.rmtree(file_dirname)
    stations_name = [station[:-4] for station in stations_list]
    request_log_path = os.path.join(os.path.dirname(file_dirname),"request_result.txt")
    if os.path.exists(request_log_path):
        os.remove(request_log_path)
    with open(request_log_path,'w') as f:
        now_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f'最后更新时间: {now_date}\n')
    for station in stations_name:
        with open(request_log_path,'a+') as f:
            f.write(f'{station}\n')
    print("解压完成...")
    root = tk.Tk()
    root.withdraw()
    root.wm_attributes('-topmost', 1)
    tkinter.messagebox.showinfo(message=f'完成{len(stations_list)}个站点报表请求.\n请前往: {request_log_path} 查看详情.')


if __name__ == "__main__":
    file_dirname = "D:/请求/zippedfile_temp"
    socket_client(file_dirname)
    unzip_manger_folder(file_dirname)
