#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/20 0020 11:48
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : showQt.py


from ui.loginTest import Ui_LoginTest
from ui.showCalender import Ui_calenderWindow
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5 import QtCore


# 初始化页面1函数
class Ui_calenderWindow(QMainWindow, Ui_calenderWindow):
    def __init__(self):
        super(Ui_calenderWindow, self).__init__()
        self.setupUi(self)


# 初始化页面2函数
class CamShow(QMainWindow, Ui_LoginTest):
    switch_to_calender = QtCore.pyqtSignal()

    def __init__(self, parent=None):
        super(CamShow, self).__init__(parent)
        self.setupUi(self)
        # self.UiComponents()
        # 跳转初始化
        self.loginIn.clicked.connect(self.goCalender)

    # def UiComponents(self):
    #     # creating a push button
    #     # adding action to a button
    #     self.loginIn.clicked.connect(self.clickme)
    #
    #     # action method

    # 跳转函数
    def goCalender(self):
        self.switch_to_calender.emit()

    def clickme(self):
        # printing pressed
        print("pressed")


# 利用一个控制器来控制页面的跳转
class Controller:
    def __init__(self):
        pass

    # 跳转到 hello 窗口
    def show_main(self):
        self.hello = CamShow()
        self.hello.switch_to_calender.connect(self.show_children)
        self.hello.show()

    # 跳转到 login 窗口, 注意关闭原页面
    def show_children(self):
        self.calender = Ui_calenderWindow()
        self.calender.show()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    controller = Controller()
    controller.show_main()  # 默认展示的是 hello 页面
    sys.exit(app.exec_())
