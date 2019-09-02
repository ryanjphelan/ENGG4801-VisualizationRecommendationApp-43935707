# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'queryRecommendations.ui'
#
# Created by: PyQt5 UI code generator 5.11.3
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_Form(object):
    def setupUi(self, Form):
        Form.setObjectName("Form")
        Form.resize(455, 491)
        self.queryRecommendations = QtWidgets.QListWidget(Form)
        self.queryRecommendations.setGeometry(QtCore.QRect(10, 70, 431, 411))
        font = QtGui.QFont()
        font.setPointSize(10)
        self.queryRecommendations.setFont(font)
        self.queryRecommendations.setObjectName("queryRecommendations")
        self.returnQueryRecommendation = QtWidgets.QPushButton(Form)
        self.returnQueryRecommendation.setGeometry(QtCore.QRect(290, 10, 151, 51))
        self.returnQueryRecommendation.setObjectName("returnQueryRecommendation")
        self.label = QtWidgets.QLabel(Form)
        self.label.setGeometry(QtCore.QRect(20, 10, 261, 51))
        self.label.setWordWrap(True)
        self.label.setObjectName("label")

        self.retranslateUi(Form)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        _translate = QtCore.QCoreApplication.translate
        Form.setWindowTitle(_translate("Form", "Query Recommendations"))
        self.returnQueryRecommendation.setText(_translate("Form", "RETURN SELECTED QUERY"))
        self.label.setText(_translate("Form", "NOTE: these query recommendations are only meant to serve as a starting point. You are encouraged to expand on these to generate richer data visualization recommendations."))


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    Form = QtWidgets.QWidget()
    ui = Ui_Form()
    ui.setupUi(Form)
    Form.show()
    sys.exit(app.exec_())

