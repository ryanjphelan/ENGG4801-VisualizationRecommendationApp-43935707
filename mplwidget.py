# ------------------------------------------------- ----- 
# -------------------- mplwidget.py -------------------- 
# -------------------------------------------------- ---- 
from  PyQt5.QtWidgets  import *
from PyQt5 import QtCore, QtGui, QtWidgets, uic
from PyQt5.QtWidgets import QInputDialog, QLineEdit, QFileDialog, QGridLayout
from PyQt5.QtGui import QIcon
from  matplotlib.backends.backend_qt5agg  import  FigureCanvas
from  matplotlib.figure  import  Figure
from  matplotlib.backends.backend_qt5agg  import  ( NavigationToolbar2QT  as  NavigationToolbar )

    
class MplWidget(QWidget):

		def __init__(self, parent = None):

			QWidget.__init__(self, parent)

			self.canvas = FigureCanvas(Figure(constrained_layout=True))
			self.canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
			print(self.canvas.figure.get_size_inches())
			self.vertical_layout = QVBoxLayout() 
			self.vertical_layout.addStretch(1)
			#self.canvas.axes = self.canvas.figure.add_subplot(111) 
			
			self.toolbar = NavigationToolbar(self.canvas, self)
			self.vertical_layout.addWidget(self.toolbar, stretch=0)
			self.vertical_layout.addWidget(self.canvas, QtCore.Qt.AlignTop)
			self.k = None

			self.setLayout(self.vertical_layout)
			#self.addToolBar(NavigationToolbar(self.MplWidget.canvas, self))

		def setAxes(self, k):
			self.k = k
			self.canvas.axes = self.canvas.figure.add_subplot(k, 1, 1)
			return self.canvas.axes

		def getAxes(self, pos):
			self.canvas.axes = self.canvas.figure.add_subplot(self.k, 1, pos)
			return self.canvas.axes

		def getOnlyAxes(self):
			return self.canvas.axes