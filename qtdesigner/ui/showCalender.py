# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'showCalender.ui'
#
# Created by: PyQt5 UI code generator 5.15.2
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_calenderWindow(object):
    def setupUi(self, calenderWindow):
        calenderWindow.setObjectName("calenderWindow")
        calenderWindow.resize(800, 600)
        self.centralwidget = QtWidgets.QWidget(calenderWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.calendarWidget = QtWidgets.QCalendarWidget(self.centralwidget)
        self.calendarWidget.setGeometry(QtCore.QRect(190, 90, 311, 241))
        self.calendarWidget.setObjectName("calendarWidget")
        calenderWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(calenderWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 800, 23))
        self.menubar.setObjectName("menubar")
        calenderWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(calenderWindow)
        self.statusbar.setObjectName("statusbar")
        calenderWindow.setStatusBar(self.statusbar)

        self.retranslateUi(calenderWindow)
        QtCore.QMetaObject.connectSlotsByName(calenderWindow)

    def retranslateUi(self, calenderWindow):
        _translate = QtCore.QCoreApplication.translate
        calenderWindow.setWindowTitle(_translate("calenderWindow", "MainWindow"))
