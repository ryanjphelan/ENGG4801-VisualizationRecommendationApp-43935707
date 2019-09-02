import sys
from functions import *
from PyQt5 import QtCore, QtGui, QtWidgets, uic
from PyQt5.QtWidgets import QInputDialog, QLineEdit, QFileDialog, QGridLayout
from PyQt5.QtGui import QIcon
#from PyQt5 import QtCore, QtGui, QtWidgets, uic 
#from PyQt5.QtGui import QFileDialog
import pandas
import pandasql
import sqlite3
from sqlite3 import Error
import math 
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from  matplotlib.backends.backend_qt5agg  import  ( NavigationToolbar2QT  as  NavigationToolbar )

class Window(QtWidgets.QWidget):

    def __init__(self):
        super().__init__()
        self.dlg = None
        self.init_ui()
        self.dataBase = None
        self.dataBaseSub = None
        self.preview = None
        self.recommendations = None
        self.dimensions = None
        self.measures = None
        self.conn1 = None
        self.conn2 = None
        self.cur1 = None
        self.cur2 = None
        self.toolBar = None
    
    def init_ui(self):
        """Import the QtDesigner file and assign functions to each of the UI's buttons.
        """
        self.dlg = uic.loadUi("interface.ui")
        self.dlg.show()
        self.dlg.browseForFileButton.clicked.connect(self.browse_for_file)
        self.dlg.importDataButton.clicked.connect(self.import_file)
        self.dlg.previewDataButton.clicked.connect(self.preview_file)
        self.dlg.assignAsDimensionButton.clicked.connect(self.assign_as_dimension)
        self.dlg.assignAsMeasureButton.clicked.connect(self.assign_as_measure)
        self.dlg.removeDimensionAssignmentButton.clicked.connect(self.remove_dimension_assignment)
        self.dlg.removeMeasureAssignmentButton.clicked.connect(self.remove_measure_assignment)
        self.dlg.generateRecommendationsButton.clicked.connect(self.generateRecommendations)
        self.dlg.executeRefQueryButton.clicked.connect(self.execute_ref_query)
        self.dlg.previewRefQueryButton.clicked.connect(self.preview_ref_query)
        self.dlg.generateVisualizationsButton.clicked.connect(self.outputToPane)
        #self.dlg.MplWidget.addToolBar(NavigationToolbar(self.dlg.MplWidget.canvas, self))
        #self.toolBar = self.dlg.addToolBar(NavigationToolbar(self.dlg.MplWidget.canvas, self))

    def browse_for_file(self):
        """Open a windows File-Dialog to allow the user to simultaneously define the filepath and the filename
        """
        fileName = QtWidgets.QFileDialog.getOpenFileName(self, 'OpenFile')
        self.dlg.fileLocation.setText(fileName[0])

    def create_connection(self, db_file):
        """Set objects for conn1 (Connection 1) and cur1 (Connection 1's cursor)

        Args:
            db_file: the data file being connected to.
        """
        try:
            self.conn1 = sqlite3.connect(db_file)   #Set Connection 1
            self.cur1 = self.conn1.cursor()         #Set Cursor 1
            print(sqlite3.version)
        except Error as e:
            print(e)

    def create_connection2(self, db_file):
        """Set objects for conn2 (Connection 2) and cur2 (Connection 2's cursor)
        Two connections are needed because two subsets of the data file are being compared.

        Args:
            db_file: the data file being connected to.
        """
        try:
            self.conn2 = sqlite3.connect(db_file)   #Set Connection 2
            self.cur2 = self.conn1.cursor()         #Set Cursor 2
            print(sqlite3.version)
        except Error as e:
            print(e)

    def import_file(self):
        """Import csv, Retrieve column names, create the connection, convert database to SQL.
        """
        filePath = self.dlg.fileLocation.text()      #Select the file path directly from the textbox
        if not filePath : return                     #Check if not null
        self.dataBase = pandas.read_csv(filePath)    #Create the pandas dataframe using .read_csv
        self.dlg.columnsListWidget.addItems(self.dataBase.columns)  #Add the column names to the columnsListWidget
        self.create_connection('dataBaseRef.db')     #Create the connection to dataBaseRef.db
        self.dataBase.to_sql("dataBaseRef", self.conn1, if_exists='replace', index=False) #Convert to SQL database

    def preview_file(self):
        """Load the database into a temporary widget to let the user 'preview' their data and ensure its correctness.
        Exit function if they haven't defined a filepath.
        """
        if self.dataBase is None : return
        model = PandasModel(self.dataBase)  #Create a model of the database
        self.preview = QtWidgets.QWidget()  #Create the 'preview' widget
        ui = TableView()                    #Create a TableView for the UI
        ui.setupUi(self.preview, model)     #Load the model into the created widget
        self.preview.show()                 #Show the widget

    def assign_as_dimension(self):
        """Take the currently selected item in columnsListWidget and assign it to a dimension.
        """
        if self.dlg.columnsListWidget.currentItem() != None:
            self.dlg.dimensionsListWidget.addItem(self.dlg.columnsListWidget.currentItem().text())  #Add to dimensions
            self.dlg.columnsListWidget.takeItem(self.dlg.columnsListWidget.currentRow())    #Remove from columns

    def assign_as_measure(self):
        """Take the currently selected item in columnsListWidget and assign it to be a measure.
        """
        if self.dlg.columnsListWidget.currentItem() != None:
            self.dlg.measuresListWidget.addItem(self.dlg.columnsListWidget.currentItem().text())    #Add to measures  
            self.dlg.columnsListWidget.takeItem(self.dlg.columnsListWidget.currentRow())    #Remove from columns

    def remove_dimension_assignment(self):
        """Remove the currently selected dimension and re-add it to the columnsListWidget.
        """
        if self.dlg.dimensionsListWidget.currentItem() != None:
            self.dlg.columnsListWidget.addItem(self.dlg.dimensionsListWidget.currentItem().text())  #Add to columns.   
            self.dlg.dimensionsListWidget.takeItem(self.dlg.dimensionsListWidget.currentRow())  #Remove from dimensions.

    def remove_measure_assignment(self):
        """Remove the currently selected measure and re-add it to the columnsListWidget.
        """
        if self.dlg.measuresListWidget.currentItem() != None:
            self.dlg.columnsListWidget.addItem(self.dlg.measuresListWidget.currentItem().text())    #Add to columns.
            self.dlg.measuresListWidget.takeItem(self.dlg.measuresListWidget.currentRow())  #Remove from measures.

    def generateRecommendations(self):
        """Generate recommendations that the user can choose from to define their subset database.
        Method utilises the 'functions.py' file for the methods generating combinations.
        """
        self.dimensions = []    #Create a list of DIMENSIONS and add all dimensions chosen by user to it
        for index in range(self.dlg.dimensionsListWidget.count()):
            self.dimensions.append(self.dlg.dimensionsListWidget.item(index).text())
        self.measures = []      #Create a list of MEASURES and add all dimensions chosen by user to it
        for index in range(self.dlg.measuresListWidget.count()):
            self.measures.append(self.dlg.measuresListWidget.item(index).text())
        #Generate the recommendations as a list of SQL Strings
        recommendationsList = generateQueryRecommendations(self.dataBase, self.dimensions, self.measures)
        self.recommendations = QtWidgets.QWidget()  #Create a new widget
        ui = RecommendationsForm()                  #Use the recommendations form object 
        ui.setupUi(self.recommendations)            #Set-up the UI
        ui.addQueryItems(recommendationsList)       #Add the items to the list
        self.recommendations.show()                 #Show the recommendations as a pop-up widget to the user

    def execute_ref_query(self):
        """Method executes the user-defined query as an SQL query against the imported database.
        This new database will be Connection 2.

        TODO Need to add error checking to this so it doesn't brick when the query is bogus
        """
        query = self.dlg.refQueryInput.toPlainText()                #Get the query from the textbox
        print(query)
        self.dataBaseSub = pandas.read_sql_query(query, self.conn1) #Read query using pandas method
        self.create_connection2('dataBaseSub.db')                   #Create Connected 2
        self.dataBaseSub.to_sql("dataBaseSub", self.conn2, if_exists='replace', index=False) #Make database SQL

    def preview_ref_query(self):
        """Load dataBaseSub into a temporary widget and'preview' the data in a pop-up widget.
        Exit function if dataBaseSub isn't defined.
        """
        if self.dataBaseSub is None : return
        model = PandasModel(self.dataBaseSub)   #Create a model of the database
        self.preview = QtWidgets.QWidget()      #Create a TableView for the UI
        ui = TableView()                        #Create the 'preview' widget
        ui.setupUi(self.preview, model)         #Load the model into the created widget
        self.preview.show()                     #Show the widget          

    def get_aggregate_functions(self):
        """Helper method to convert all selected aggregate function buttons into a list of strings.

         Returns:
            A list of strings, each string represents an SQL aggregate function.
        """
        functions = []
        # If selected, add the NAME of the button to the list
        if self.dlg.avgButton.isChecked() : functions.append(self.dlg.avgButton.text())     #AVG
        if self.dlg.countButton.isChecked() : functions.append(self.dlg.countButton.text()) #COUNT
        if self.dlg.minButton.isChecked() : functions.append(self.dlg.minButton.text())     #MIN
        if self.dlg.maxButton.isChecked() : functions.append(self.dlg.maxButton.text())     #MAX
        if self.dlg.sumButton.isChecked() : functions.append(self.dlg.sumButton.text())     #SUM
        if self.dlg.otherFunctionButton.currentText() != 'other...' :  #STDEV, MEDIAN, MODE, or RANGE
            functions.append(self.dlg.otherFunctionButton.currentText())
        return functions
    
    def calculate_utility(self, combinedDataFrame):
        total = 0
        #Loop through and calculate the difference of the two scores squared
        for index, row in combinedDataFrame.iterrows():
            total = total + (row[combinedDataFrame.columns[1]] - row[combinedDataFrame.columns[2]])**2
        #Return the utlity by taking the square root of the total
        return math.sqrt(total)

    def getKey(self, item):
        return item[1]

    def calculate_all_utilities(self, queriesTuple):
        #The return list that will have a list of tuples containing the dataframe and its calculated utility
        allViews = []
        #Loop through all of the tuples
        for queryTar,queryRef in queriesTuple:
            #The subset query will be the first query in the tuple. Execute on conn2
            targetDataframe = pandas.read_sql(queryTar, self.conn2)
            newColName1 = targetDataframe.columns[1] + " tar"
            targetDataframe.columns = [targetDataframe.columns[0], newColName1]
            #The reference query will be the second. Execute on conn1
            referenceDataFrame = pandas.read_sql(queryRef, self.conn1)
            newColName2 = referenceDataFrame.columns[1] + " ref"
            referenceDataFrame.columns = [referenceDataFrame.columns[0], newColName2]
            #Combine the two dataframes, this will be necesarry to graph them
            combined = pandas.merge(targetDataframe, referenceDataFrame, on=targetDataframe.columns[0], how='outer')
            #print(combined.columns)
            #combined.set_index(combined.columns[0],drop=True,inplace=True)
            #combined.drop(['0'], axis=1)
            combined.fillna(0, inplace=True)
            combined.sort_values(by=[combined.columns[0]])
            utility = self.calculate_utility(combined)
            view = (combined, utility)
            print(view)
            allViews.append(view)

        sortedViews = sorted(allViews, key = lambda x: x[1])
        return sortedViews
        
    def plot_visualizations(self, allViews):
        #Loop through all the views until the number of plots equals k
        k = int(str(self.dlg.kValueBox.currentText()))
        numberOfPlots = k
        """ax = self.dlg.MplWidget.setAxes(k)"""
        ax = self.dlg.MplWidget.setAxes(1)
        self.dlg.MplWidget.canvas.axes.clear()
        """for view, utility in allViews:
            if k == numberOfPlots:
                view.plot.bar(rot=0, ax=ax)
                k = k - 1
                continue
            elif k == 0:
                break
            else:
                ax = self.dlg.MplWidget.getAxes(numberOfPlots - (k - 1))
                view.plot.bar(rot=0, ax=ax)
                k = k - 1
            """
        allViews[0][0].plot.bar(rot=0, ax=ax)
        self.dlg.MplWidget.canvas.draw()
        #self.dlg.MplWidget.canvas.draw()



    def outputToPane(self):
        #Get the list of dimension attributes
        self.dimensions = []
        for index in range(self.dlg.dimensionsListWidget.count()):
            self.dimensions.append(self.dlg.dimensionsListWidget.item(index).text())
        #Get the list of measure attributes
        self.measures = []
        for index in range(self.dlg.measuresListWidget.count()):
            self.measures.append(self.dlg.measuresListWidget.item(index).text())
        #Generate the list of aggregate functions
        aggregateFunctions = self.get_aggregate_functions()
        #print("%s, %s, %s, %s, %s", (self.dimensions, aggregateFunctions, self.measures, "dataBaseSub", "dataBaseRef"))
        #Checks and balances to ensure that the queries should be generated and not error
        if not aggregateFunctions or not self.dimensions or not self.measures :
            return
        queries = generateQueriesAsViewTuples(self.dimensions, aggregateFunctions, self.measures, "dataBaseSub", "dataBaseRef")
        allViews = self.calculate_all_utilities(queries)
        self.plot_visualizations(allViews)
        #self . MplWidget . canvas . axes . legend (( 'cosinus' ,  'sinus' ),loc = 'upper right' ) 
        #self . MplWidget . canvas . axes . set_title ( ' Cosinus - Sinus Signal' ) 
        #self.dlg.visualsPane.addItems(ax)
        #for view in queries:
        #    print (view)


class PandasModel(QtCore.QAbstractTableModel):
    """
    Class to populate a table view with a pandas dataframe as a widget
    """
    def __init__(self, data, parent=None):
        QtCore.QAbstractTableModel.__init__(self, parent)
        self._data = data

    def rowCount(self, parent=None):
        return len(self._data.values)

    def columnCount(self, parent=None):
        return self._data.columns.size

    def data(self, index, role=QtCore.Qt.DisplayRole):
        if index.isValid():
            if role == QtCore.Qt.DisplayRole:
                return str(self._data.values[index.row()][index.column()])
        return None

    def headerData(self, col, orientation, role):
        if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
            return self._data.columns[col]
        return None

class TableView(object):
    """
    Class to for a TableView widget
    """
    def setupUi(self, Form, Model):
        Form.setObjectName("Data Preview")
        Form.resize(1031, 601)
        self.tableView = QtWidgets.QTableView(Form)
        self.tableView.setGeometry(QtCore.QRect(10, 10, 1007, 587))
        self.tableView.setObjectName("tableView")
        self.tableView.setModel(Model)
        self.tableView.resizeColumnsToContents()

        self.retranslateUi(Form)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        _translate = QtCore.QCoreApplication.translate
        Form.setWindowTitle(_translate("Form", "Form"))

class RecommendationsForm(object):
    """
    Class for the query recommendations pop-up
    """
    def setupUi(self, Form):
        Form.setObjectName("Form")
        Form.resize(600, 570)
        self.queryRecommendations = QtWidgets.QListWidget(Form)
        self.queryRecommendations.setGeometry(QtCore.QRect(10, 70, 575, 480))
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
        self.label.setText(_translate("Form", "NOTE: these query recommendations are only meant \
                 to serve as a starting point.                    \
                 You are encouraged to expand on these to create richer visualization recommendations."))

    def addQueryItems(self, items):
        self.queryRecommendations.addItems(items)
  
def create_window():
    app = QtWidgets.QApplication(sys.argv)
    my_window = Window()
    sys.exit(app.exec())

create_window()