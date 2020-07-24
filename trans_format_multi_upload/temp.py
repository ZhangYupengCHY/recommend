# -*- coding: utf-8 -*-
"""
Proj: recommend
Created on:   2020/1/10 10:19
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import streamlit as st

st.write('Hello World!')


x = st.slider('x')
st.write(x,'squared is',x*x)

