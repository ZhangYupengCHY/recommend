# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'QtTest.ui'
#
# Created by: PyQt5 UI code generator 5.15.2
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(800, 600)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.widget = QtWidgets.QWidget(self.centralwidget)
        self.widget.setGeometry(QtCore.QRect(80, 60, 341, 191))
        self.widget.setObjectName("widget")
        self.gridLayout = QtWidgets.QGridLayout(self.widget)
        self.gridLayout.setContentsMargins(0, 0, 0, 0)
        self.gridLayout.setObjectName("gridLayout")
        self.login = QtWidgets.QLabel(self.widget)
        self.login.setObjectName("login")
        self.gridLayout.addWidget(self.login, 0, 0, 1, 1)
        self.inputLogin = QtWidgets.QLineEdit(self.widget)
        self.inputLogin.setObjectName("inputLogin")
        self.gridLayout.addWidget(self.inputLogin, 0, 1, 1, 2)
        self.password = QtWidgets.QLabel(self.widget)
        self.password.setObjectName("password")
        self.gridLayout.addWidget(self.password, 1, 0, 1, 1)
        self.inputPassword = QtWidgets.QLineEdit(self.widget)
        self.inputPassword.setObjectName("inputPassword")
        self.gridLayout.addWidget(self.inputPassword, 1, 1, 1, 2)
        self.loginIn = QtWidgets.QPushButton(self.widget)
        self.loginIn.setObjectName("loginIn")
        self.gridLayout.addWidget(self.loginIn, 2, 0, 1, 1)
        self.findPassword = QtWidgets.QPushButton(self.widget)
        self.findPassword.setObjectName("findPassword")
        self.gridLayout.addWidget(self.findPassword, 2, 1, 1, 1)
        self.register_2 = QtWidgets.QPushButton(self.widget)
        self.register_2.setObjectName("register_2")
        self.gridLayout.addWidget(self.register_2, 2, 2, 1, 1)
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 800, 23))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        self.login.setText(_translate("MainWindow", "   账户名："))
        self.password.setText(_translate("MainWindow", "   密 码："))
        self.loginIn.setText(_translate("MainWindow", "登录"))
        self.findPassword.setText(_translate("MainWindow", "找回密码"))
        self.register_2.setText(_translate("MainWindow", "注册"))