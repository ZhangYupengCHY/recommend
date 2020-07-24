# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/5/16 14:18
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import plotly

"""
函数简介:
    matplotlib是Python中最广为人知的图表绘制框架。但是它实在太复杂了，如果要画出较为漂亮的图，
    则需要付出很多的时间与精力。这次介绍一个Python下能轻松生成各种图表的框架plotly plotly是一个可交互，
    基于浏览器的绘图库，主打功能是绘制在线可交互的图表,所绘制出来的图表真的赏心悦目。它所支持的语言不只是Python，
    还支持诸如r,matlab,javescript等语言。plotly绘制的图能直接在jupyter中查看，也能保存为离线网页，
    或者保存在plot.ly云端服务器内，以便在线查看。
"""

import plotly
import plotly.graph_objs as go

plotly.offline.plot({
    "data": [go.Bar(x=[1, 2, 3, 4],y=[4, 3, 2, 1],marker=dict(color=["#FF0000", "#00FF00","#FF0000", "#00FF00"]))],
    "layout": go.Layout(title="hello world")
}, auto_open=True)
