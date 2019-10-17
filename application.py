import sys
import os
import datetime
import time
import math
from functions import *
from PyQt5 import QtCore, QtGui, QtWidgets, uic
from PyQt5.QtWidgets import QInputDialog, QLineEdit, QFileDialog, QGridLayout
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import pyqtSignal
#from PyQt5 import QtCore, QtGui, QtWidgets, uic 
#from PyQt5.QtGui import QFileDialog
import pandas
import pandasql
import sqlite3
from sqlite3 import Error
import sqlparse
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
        self.dataBaseRef = None
        self.dataBaseTar = None
        self.dynamicDataBase = None
        self.dynamicTargetDatabase = None
        self.dynamicReferenceDatabase = None
        self.preview = None
        self.recommendations = None
        self.dimensions = None
        self.measures = None
        self.conn1 = None
        self.conn2 = None
        self.conn3 = None
        self.dynaConn = None
        self.dynaConnTarget = None
        self.dynaConnReference = None
        self.cur1 = None
        self.cur2 = None
        self.cur3 = None
        self.dynaCursor = None
        self.dynaConnTargetCursor = None
        self.dynaConnReferenceCursor = None
        self.allViews = None
        self.toolBar = None
        self.fileName = None
        self.query = None
        self.houseId = None
        self.threadClass1 = None
        self.threadRunning = False
        self.targetDatabaseCondition = None
        self.currentVisualIndex = 0
        self.maxIndex = None
        self.runAtLeastOnce = False
        self.dlg.myConsole.setFontPointSize(8)
        self.dlg.myConsole.setText(self.dlg.myConsole.toPlainText() + "\n")

    
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
        self.dlg.executeRefQueryButton.clicked.connect(self.execute_ref_query)
        self.dlg.previewRefQueryButton.clicked.connect(self.preview_target_query)
        self.dlg.previewReferenceDatabaseButton.clicked.connect(self.preview_reference_query)
        self.dlg.generateVisualizationsButton.clicked.connect(self.outputToPane)
        self.dlg.connectToHouseButton.clicked.connect(self.connectToHouse)
        self.dlg.closeDynamicConnectionButton.clicked.connect(self.closeDynamicConnection)
        self.dlg.previousVisualButton.clicked.connect(self.previousVisual)
        self.dlg.nextVisualButton.clicked.connect(self.nextVisual)
        self.dlg.previewDynamicDataButton.clicked.connect(self.preview_dynamic_data)

    def browse_for_file(self):
        """Open a windows File-Dialog to allow the user to simultaneously define the filepath and the filename
        """
        if self.threadRunning :
            self.addLineToConsole("Please close the dynamic connection before attempting to analyse static data")
            return
        fileName = QtWidgets.QFileDialog.getOpenFileName(self, 'OpenFile')
        self.dlg.fileLocation.setText(fileName[0])
        self.fileName = os.path.basename(self.dlg.fileLocation.text())


    def create_connection(self, db_file):
        """Set objects for conn1 (Connection 1) and cur1 (Connection 1's cursor)

        Args:
            db_file: the data file being connected to.
        """
        try:
            self.conn1 = sqlite3.connect(db_file)   #Set Connection 1
            self.cur1 = self.conn1.cursor()         #Set Cursor 1
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
            self.cur2 = self.conn2.cursor()         #Set Cursor 2
        except Error as e:
            print(e)

    def create_connection3(self, db_file):
        """Set objects for conn2 (Connection 2) and cur2 (Connection 2's cursor)
        Two connections are needed because two subsets of the data file are being compared.

        Args:
            db_file: the data file being connected to.
        """
        try:
            self.conn3 = sqlite3.connect(db_file)   #Set Connection 2
            self.cur3 = self.conn3.cursor()         #Set Cursor 2
        except Error as e:
            print(e)

    def create_dynamic_connection(self, db_file):
        """Set objects for dynaConn (dynamic connection) and dynaCursor (Dynamic connection's cursor)
        This set creates a connection to the current dynamic data source.

        Args:
            db_file: the dynamic data file being connected to.
        """
        try:
            self.dynaConn = sqlite3.connect(db_file)   #Set Dynamic Connection
            self.dynaCursor = self.dynaConn.cursor()   #Set Dyanamic Cursor
        except Error as e:
            print(e)        #TODO print errors out to a make shift terminal

    def create_dynamic_target_connection(self, db_file):
        try:
            self.dynaConnTarget = sqlite3.connect(db_file)   #Set Dynamic Connection
            self.dynaConnTargetCursor = self.dynaConnTarget.cursor()   #Set Dyanamic Cursor
        except Error as e:
            print(e)        #TODO print errors out to a make shift terminal

    def create_dynamic_reference_connection(self, db_file):
        try:
            self.dynaConnReference = sqlite3.connect(db_file)   #Set Dynamic Connection
            self.dynaConnReferenceCursor = self.dynaConnReference.cursor()   #Set Dyanamic Cursor
        except Error as e:
            print(e)        #TODO print errors out to a make shift terminal

    def import_file(self):
        """Import csv, Retrieve column names, create the connection, convert database to SQL.
        """
        filePath = self.dlg.fileLocation.text()      #Select the file path directly from the textbox
        if not filePath : return                     #Check if not null
        if self.threadRunning : return
        self.dataBase = pandas.read_csv(filePath, encoding = "ISO-8859-1") #Create the pandas dataframe using .read_csv
        self.dlg.columnsListWidget.addItems(self.dataBase.columns)  #Add the column names to the columnsListWidget
        self.create_connection('databases/dataBase.db')     #Create the connection to dataBase.db
        self.dataBase.to_sql("dataBase", self.conn1, if_exists='replace', index=False) #Convert to SQL database
        

    def preview_file(self):
        """Load the database into a temporary widget to let the user 'preview' their data and ensure its correctness.
        Exit function if they haven't defined a filepath.
        """
        if self.dataBase is None : return
        if self.threadRunning : return
        model = PandasModel(self.dataBase.head(100))  #Create a model of the database, only show the top 100 rows.
        self.preview = QtWidgets.QWidget()  #Create the 'preview' widget
        ui = TableView()                    #Create a TableView for the UI
        ui.setupUi(self.preview, model)     #Load the model into the created widget
        self.preview.show()                 #Show the widget
        self.preview.setWindowTitle("SELECT TOP 100 FROM " + self.fileName)

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

    def execute_ref_query(self):
        """Method executes the user-defined query as an SQL query against the imported database.
        This new database will be Connection 2.

        TODO Need to add error checking to this so it doesn't brick when the query is bogus
        """
        query = self.dlg.refQueryInput.toPlainText()                #Get the query from the textbox
        self.queryText = query
        self.targetDatabaseCondition = str(str(
                                sqlparse.parse(self.dlg.refQueryInput.toPlainText())[0][8]).split("WHERE")[1]).strip()
        if not self.threadRunning :
            if self.conn1 is None : return
            self.addLineToConsole("Creating target database...         ")
            self.dataBaseTar = pandas.read_sql_query(query, self.conn1) #Read query using pandas method
            self.create_connection2('databases/dataBaseTar.db')                   #Create Connected 2
            self.dataBaseTar.to_sql("dataBaseTar", self.conn2, if_exists='replace', index=False) #Make database SQL
            self.addRepeatingLineToConsole("Creating target database...Complete.")
            if self.dlg.complementDataSetCheckbox.isChecked() :
                print("was, is this fireing?")
                self.addLineToConsole("Creating reference database...         ")
                #The user wants the reference database to be the compliment of the target database, reverse the query
                complimentQuery = self.get_compliment_query()
                print(complimentQuery)
                self.dataBaseRef = pandas.read_sql_query(complimentQuery, self.conn1)
                self.dataBaseRef.to_csv(r'C:\Users\Ryan Phelan\Desktop\ENGG4801\ENGG4801_RyanPhelan\databases\refd.csv')
                self.create_connection3('databases/dataBaseRef.db')
                self.dataBaseRef.to_sql("dataBaseRef", self.conn3, if_exists='replace', index = False)
                self.addRepeatingLineToConsole("Creating reference database...Complete.")
        else :
            if self.dynaConn is None : return
            # The dynamic thread is currently running, execute on the current house's database
            self.addLineToConsole("Creating target database...         ")
            self.dynamicTargetDatabase = pandas.read_sql_query(query, self.dynaConn)
            self.create_dynamic_target_connection('databases/id' + str(self.houseId) + 'Target.db')
            self.dynamicTargetDatabase.to_sql("id" + str(self.houseId) + "Target", self.dynaConnTarget, 
                                                if_exists='replace', index = False)
            self.addRepeatingLineToConsole("Creating target database...Complete.")
            if self.dlg.complementDataSetCheckbox.isChecked() :
                self.addLineToConsole("Creating reference database...         ")
                #The user wants the reference database to be the compliment of the target database, reverse the query
                complimentQuery = self.get_compliment_query()
                print(complimentQuery)
                self.dynamicReferenceDatabase = pandas.read_sql_query(complimentQuery, self.dynaConn)
                self.create_dynamic_reference_connection('databases/id' + str(self.houseId) + 'Reference.db')
                self.dynamicReferenceDatabase.to_sql("id" + str(self.houseId) + "Reference", self.dynaConnReference, 
                                                if_exists='replace', index = False)
                self.addRepeatingLineToConsole("Creating reference database...Complete.")

    def get_compliment_query(self):
        queryString = self.dlg.refQueryInput.toPlainText()
        if queryString.find("WHERE") == -1 :
            self.addLineToConsole("No 'WHERE' clause detected in query. Please try again.")
            return
        parsedQuery = sqlparse.parse(queryString)
        statement = parsedQuery[0]
        columnsWanted = str(statement[2])
        tableName = str(statement[6])
        whereClause = str(statement[8])
        clause = whereClause.split("WHERE")[1]
        sqliteValidOperators = ["==", "=", "!=", "<>", ">", "<", ">=", "<=", "!<", "!>"]
        columnIdentifier = ""
        clauseOperator = ""
        valueComparingTo = ""
        for operator in sqliteValidOperators:
            if clause.find(operator) == -1:
                continue
            else :
                columnIdentifier = clause.split(operator)[0]
                clauseOperator = operator
                valueComparingTo = clause.split(operator)[1]
        complimentQuery = ("SELECT " + columnsWanted + " FROM " + tableName + " WHERE " + columnIdentifier 
                            + " NOT IN " + "( SELECT " + columnIdentifier.strip() + " FROM " + tableName + " WHERE "
                            + columnIdentifier + " " + clauseOperator + " " + valueComparingTo + ")")
        self.targetDatabaseCondition = str(whereClause.split("WHERE")[1].strip())
        return complimentQuery 

    def preview_target_query(self):
        """Load target into a temporary widget and'preview' the data in a pop-up widget.
        Exit function if the database isn't defined.
        """
        if not self.threadRunning :
            if self.dataBaseTar is None : return
            model = PandasModel(self.dataBaseTar.head(100))   #Create a model of the database, only get TOP 100 rows
            self.preview = QtWidgets.QWidget()      #Create a TableView for the UI
            ui = TableView()                        #Create the 'preview' widget
            ui.setupUi(self.preview, model)         #Load the model into the created widget
            self.preview.show()                     #Show the widget       
            self.preview.setWindowTitle("SELECT TOP 100 FROM Target DataSet")   
        else :
            if self.dynamicTargetDatabase is None : return
            model = PandasModel(self.dynamicTargetDatabase.head(100))   #Create a model of the database, top 100 rows
            self.preview = QtWidgets.QWidget()      #Create a TableView for the UI
            ui = TableView()                        #Create the 'preview' widget
            ui.setupUi(self.preview, model)         #Load the model into the created widget
            self.preview.show()                     #Show the widget       
            self.preview.setWindowTitle("SELECT TOP 100 FROM Target DataSet")

    def preview_reference_query(self):
        """Load reference into a temporary widget and'preview' the data in a pop-up widget.
        Exit function if the database isn't defined.
        """
        if not self.threadRunning :
            if self.dataBaseRef is None : return
            model = PandasModel(self.dataBaseRef.head(100))   #Create a model of the database, top 100 rows
            self.preview = QtWidgets.QWidget()      #Create a TableView for the UI
            ui = TableView()                        #Create the 'preview' widget
            ui.setupUi(self.preview, model)         #Load the model into the created widget
            self.preview.show()                     #Show the widget       
            self.preview.setWindowTitle("SELECT TOP 100 FROM Reference DataSet")
            return
        else :
            if self.dynamicReferenceDatabase is None : return
            model = PandasModel(self.dynamicReferenceDatabase.head(100))   #Create a model of the database, top 100 rows
            self.preview = QtWidgets.QWidget()      #Create a TableView for the UI
            ui = TableView()                        #Create the 'preview' widget
            ui.setupUi(self.preview, model)         #Load the model into the created widget
            self.preview.show()                     #Show the widget       
            self.preview.setWindowTitle("SELECT TOP 100 FROM Reference DataSet")

    def preview_dynamic_data(self):
        if not self.threadRunning : return
        else :
            previewMeterData = pandas.read_sql("SELECT * FROM (SELECT * FROM id" + str(self.houseId) + " ORDER BY TimeStamp DESC LIMIT 100) ORDER BY TimeStamp", con=self.dynaConn)
            model = PandasModel(previewMeterData.head(100))   #Create a model of the database, top 100 rows
            self.preview = QtWidgets.QWidget()      #Create a TableView for the UI
            ui = TableView()                        #Create the 'preview' widget
            ui.setupUi(self.preview, model)         #Load the model into the created widget
            self.preview.show()                     #Show the widget       
            self.preview.setWindowTitle("SELECT BOTTOM 100 ROWS FROM Dynamic DataSet")
                

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
        return functions
    
    def calculate_utility(self, combinedDataFrame):
        total = 0
        targetMinimum = abs(combinedDataFrame[combinedDataFrame.columns[1]].min())
        if targetMinimum >= 0 : targetMinimum = 0
        targetValues = [x + targetMinimum for x in combinedDataFrame[combinedDataFrame.columns[1]].values]
        targetSum = sum(targetValues)
        referenceMinimum = abs(combinedDataFrame[combinedDataFrame.columns[2]].min())
        if referenceMinimum >= 0 : referenceMinimum = 0
        referenceValues = [x + referenceMinimum for x in combinedDataFrame[combinedDataFrame.columns[2]].values]
        referenceSum = sum(referenceValues)
        #Loop through and calculate the difference of the two scores squared
        for target, reference in zip(targetValues, referenceValues):
            targetNormalized = target / targetSum
            referenceNormalized = reference / referenceSum
            total = total + (targetNormalized - referenceNormalized)**2
        return math.sqrt(total)

    def getKey(self, item):
        return item[1]

    def calculate_all_utilities(self, queriesTuple):
        print("Starting : calculate_all_utilities")
        self.addLineToConsole("Calculating visualization utilities...Count = " + str(len(queriesTuple)))
        #The return list that will have a list of tuples containing the dataframe and its calculated utility
        self.allViews = []
        index = 0
        for queryTar,queryRef in queriesTuple:
            print(queryTar + '\n' +queryRef)
            #The subset query will be the first query in the tuple. Execute on conn2
            if not self.threadRunning :
                targetDataframe = pandas.read_sql(queryTar, con=self.conn2)
                newColName1 = targetDataframe.columns[1] + " tar"
                targetDataframe.columns = [targetDataframe.columns[0], newColName1]
                if self.dlg.complementDataSetCheckbox.isChecked() :
                    referenceDataFrame = pandas.read_sql(queryRef, con=self.conn3)
                    newColName2 = referenceDataFrame.columns[1] + " ref"
                    referenceDataFrame.columns = [referenceDataFrame.columns[0], newColName2]
                else :
                    referenceDataFrame = pandas.read_sql(queryRef, con=self.conn1)
                    newColName2 = referenceDataFrame.columns[1] + " ref"
                    referenceDataFrame.columns = [referenceDataFrame.columns[0], newColName2]
            else :
                targetDataframe = pandas.read_sql(queryTar, con=self.dynaConnTarget)
                newColName1 = targetDataframe.columns[1] + " tar"
                targetDataframe.columns = [targetDataframe.columns[0], newColName1]
                if self.dlg.complementDataSetCheckbox.isChecked() :
                    referenceDataFrame = pandas.read_sql(queryRef, con=self.dynaConnReference)
                    newColName2 = referenceDataFrame.columns[1] + " ref"
                    referenceDataFrame.columns = [referenceDataFrame.columns[0], newColName2]
                else :
                    referenceDataFrame = pandas.read_sql(queryRef, con=self.dynaConn)
                    newColName2 = referenceDataFrame.columns[1] + " ref"
                    referenceDataFrame.columns = [referenceDataFrame.columns[0], newColName2]
            #The reference query will be the second. Execute on conn1
            #Combine the two dataframes, this will be necesarry to graph them
            combined = pandas.merge(targetDataframe, referenceDataFrame, on=targetDataframe.columns[0], how='outer')
            if index == 4:
                combined.to_csv(r'C:\Users\Ryan Phelan\Desktop\ENGG4801\ENGG4801_RyanPhelan\databases\combined.csv')
            index = index + 1
            combined.fillna(0, inplace=True)
            combined.sort_values(by=[combined.columns[0]])
            utility = self.calculate_utility(combined)
            splitString = queryTar.split("FROM")[0].split("SELECT")[1].strip()
            chosenFunction = ""
            for function in ["AVG", "COUNT", "MIN", "MAX", "SUM"] :
                if splitString.find(function) != -1 :
                    chosenFunction = function
                    splitString = splitString.split(function)
                    break
            xAxis = splitString[0].strip(", ")
            yAxis = chosenFunction + splitString[1]
            databaseName = self.dlg.refQueryInput.toPlainText().split("FROM")[1].split("WHERE")
            optionTitle = queryTar.split("FROM")[0]
            title =  yAxis + " FROM" + databaseName[0] + "   Utility : " + str(round(utility, 3))
            view = (title, combined, utility, xAxis, yAxis)
            #print(view)
            self.allViews.append(view)

        self.allViews = sorted(self.allViews, key = lambda x: x[2], reverse=True)
        print("Ended : calculate_all_utilities")
        return self.allViews

    def stringTesting(self):
        s = "SELECT strftime('%H', TimeStamp), AVG(abs('PowerFactor')) FROM id4Target GROUP BY strftime('%H', TimeStamp)"
        splitString = s.split("FROM")[0].split("SELECT")[1].strip()
        functions = ["AVG", "COUNT", "MIN", "MAX", "SUM"]
        chosenFunction = ""
        for function in functions :
            if splitString.find(function) != -1 :
                chosenFunction = function
                splitString = splitString.split(function)
                break
        xAxis = splitString[0].strip(", ")
        yAxis = chosenFunction + splitString[1]
        print(splitString)    
        print(xAxis)
        print(yAxis)
    
    def plot_visualizations(self):
        self.addLineToConsole("Calculations complete. Plotting results.")
        plt.clf()
        k = int(str(self.dlg.kValueBox.currentText()))
        print(k)
        self.dlg.MplWidget.canvas.figure.clf()
        ax = self.dlg.MplWidget.setAxes(1) #set the axes to have a single subplot.
        #self.dlg.MplWidget.canvas.axes.clear()
        ax.title.set_size(40)
        print(self.allViews[0][3] == "strftime('%w', TimeStamp)")
        if self.allViews[0][3].strip() == "strftime('%w', TimeStamp)" :
            xAxis = "Day of the Week (Sunday = 0)"
        elif self.allViews[0][3].strip() == "strftime('%H', TimeStamp)" :
            xAxis = "Hour of the Day"
        elif self.allViews[0][3].strip() == "strftime('%m', TimeStamp)" :
            xAxis = "Month of the Year"
        else :
            xAxis = self.allViews[0][3]
        ax.xaxis.set_tick_params(labelsize=7)
        ax.set_ylabel(self.allViews[0][4])
        self.allViews[0][1].set_index(self.allViews[0][1].columns[0]).plot.bar(rot=60, ax=ax, title=self.allViews[0][0])
        plt.tight_layout()
        ax.legend(["Target Dataset", "Reference Dataset"]);
        ax.margins(0.15)
        ax.set_xlabel(xAxis)
        self.dlg.MplWidget.canvas.draw()
        print("Ended : plot_visualizations")

    def previousVisual(self):
        print("going to previous visual")
        if self.allViews is None :return #allviews has not been set yet
        elif self.currentVisualIndex == 0 : return #there are no visuals past these indices
        self.currentVisualIndex = self.currentVisualIndex - 1
        plt.clf()
        k = int(str(self.dlg.kValueBox.currentText()))
        print(k)
        self.dlg.MplWidget.canvas.figure.clf()
        ax = self.dlg.MplWidget.setAxes(1) #set the axes to have a single subplot.
        #self.dlg.MplWidget.canvas.axes.clear()
        ax.title.set_size(40)
        if self.allViews[self.currentVisualIndex][3] == "strftime('%w', TimeStamp)" :
            xAxis = "Day of the Week (Sunday = 0)"
        elif self.allViews[self.currentVisualIndex][3] == "strftime('%H', TimeStamp)" :
            xAxis = "Hour of the Day"
        elif self.allViews[self.currentVisualIndex][3] == "strftime('%m', TimeStamp)" :
            xAxis = "Month of the Year"
        else :
            xAxis = self.allViews[self.currentVisualIndex][3]
        ax.xaxis.set_tick_params(labelsize=7)
        ax.set_ylabel(self.allViews[self.currentVisualIndex][4])
        self.allViews[self.currentVisualIndex][1].set_index(self.allViews[self.currentVisualIndex][1].columns[0]).plot.bar(rot=60, ax=ax, title=self.allViews[self.currentVisualIndex][0])
        plt.tight_layout()
        ax.legend(["Target Dataset", "Reference Dataset"]);
        ax.margins(0.15)
        ax.set_xlabel(xAxis)
        self.dlg.MplWidget.canvas.draw()

    def nextVisual(self):
        print("going to next visual")
        if self.allViews is None :return #allviews has not been set yet
        elif self.currentVisualIndex == self.maxIndex : return #there are no visuals past these indices
        self.currentVisualIndex = self.currentVisualIndex + 1
        plt.clf()
        self.dlg.MplWidget.canvas.figure.clf()
        ax = self.dlg.MplWidget.setAxes(1) #set the axes to have a single subplot.
        #self.dlg.MplWidget.canvas.axes.clear()
        ax.title.set_size(40)
        if self.allViews[self.currentVisualIndex][3] == "strftime('%w', TimeStamp)" :
            xAxis = "Day of the Week (Sunday = 0)"
        elif self.allViews[self.currentVisualIndex][3] == "strftime('%H', TimeStamp)" :
            xAxis = "Hour of the Day"
        elif self.allViews[self.currentVisualIndex][3] == "strftime('%m', TimeStamp)" :
            xAxis = "Month of the Year"
        else :
            xAxis = self.allViews[self.currentVisualIndex][3]
        ax.xaxis.set_tick_params(labelsize=7)
        ax.set_ylabel(self.allViews[self.currentVisualIndex][4])
        self.allViews[self.currentVisualIndex][1].set_index(self.allViews[self.currentVisualIndex][1].columns[0]).plot.bar(rot=60, ax=ax, title=self.allViews[self.currentVisualIndex][0])
        plt.tight_layout()
        ax.legend(["Target Dataset", "Reference Dataset"]);
        ax.margins(0.15)
        ax.set_xlabel(xAxis)
        self.dlg.MplWidget.canvas.draw()


    def outputToPane(self):
        self.runAtLeastOnce = True
        print("Starting : outputToPane")
        #Get the list of dimension attributes
        if self.threadRunning and self.dynamicTargetDatabase is None : 
            self.addLineToConsole("Target database is not initialized. Please execute your query before proceeding.")
            return
        elif not self.threadRunning and self.dataBaseTar is None : 
            self.addLineToConsole("Target database is not initialized. Please execute your query before proceeding.")
            return
        self.dimensions = []
        for index in range(self.dlg.dimensionsListWidget.count()):
            self.dimensions.append(self.dlg.dimensionsListWidget.item(index).text())
        #Get the list of measure attributes
        self.measures = []
        for index in range(self.dlg.measuresListWidget.count()):
            self.measures.append(self.dlg.measuresListWidget.item(index).text())
        #Generate the list of aggregate functions
        aggregateFunctions = self.get_aggregate_functions()
        #Checks and balances to ensure that the queries should be generated and not error
        if not aggregateFunctions or not self.dimensions or not self.measures :
            return
        if not self.threadRunning :
            if self.dlg.complementDataSetCheckbox.isChecked() :
                queries = generateQueriesAsViewTuples(self.dimensions, aggregateFunctions, self.measures, "dataBaseTar", "dataBaseRef")
            else :
                queries = generateQueriesAsViewTuples(self.dimensions, aggregateFunctions, self.measures, "dataBaseTar", "dataBase")
        else :
            #Alter the dimensions because TimeStamp needs to be scaled down to make better visualizations"
            redbackDimensions = ["strftime('%m', TimeStamp)", "strftime('%H', TimeStamp)", "strftime('%w', TimeStamp)"]
            for timeStampDim in redbackDimensions :
                if self.targetDatabaseCondition.find(timeStampDim) != -1 :
                    redbackDimensions.remove(timeStampDim)
            if self.dlg.complementDataSetCheckbox.isChecked() :
                queries = generateQueriesAsViewTuples(redbackDimensions, aggregateFunctions, self.measures, 
                        "id" + str(self.houseId) + "Target", "id" + str(self.houseId) + "Reference")
            else :
                queries = generateQueriesAsViewTuples(redbackDimensions, aggregateFunctions, self.measures, 
                        "id" + str(self.houseId) + "Target", "id" + str(self.houseId))
            
        self.allViews = self.calculate_all_utilities(queries)
        self.maxIndex = len(self.allViews) - 1
        self.plot_visualizations()
        print("Ended : outputToPane")

    def connectToHouse(self):
        if self.threadRunning : 
            self.addLineToConsole("Connection is already running. Press 'Close Connection' before changing databases.")
            return
        #TODO NEED TO CHECK THAT YOU HAVE A VALID INTERNET CONNECTION, THROW EXCEPTION
        idString = self.dlg.houseidComboBox.currentText()   #get currently selected house ID
        self.houseId = int(idString.split()[2])  #convert id string to an integer, split string to get at number
        self.addLineToConsole("Connecting to House " + str(self.houseId) + "...")
        #Check that the databases folder exists, if not, create it
        if not os.path.isdir('./databases') :
            os.mkdir("./databases")
        #Connect to the dynamic dataset, the name of the database will be based on the house ID
        houseDatabaseString = "databases/id" + str(self.houseId) + ".db"
        self.create_dynamic_connection(houseDatabaseString)
        #If the database has no tables initialised in it, initialise the first day for the given ID 
        self.dynaCursor.execute('SELECT name from sqlite_master where type= "table"')
        if self.dynaCursor.fetchall() == [] :
            self.addLineToConsole("Initializing House " + str(self.houseId) + " table in database...")
            self.dynamicDataBase = initialiseRedbackHouse(self.houseId)
            self.dynamicDataBase.to_sql("id" + str(self.houseId), self.dynaConn, if_exists='fail', index=False)   
            self.dynaConn.commit()  
        self.dynaCursor.execute("SELECT * FROM id" + str(self.houseId) + " ORDER BY TimeStamp DESC LIMIT 1")
        lastReadDate = self.dynaCursor.fetchone()[6]
        self.addLineToConsole("LAST KNOWN METER READING: " + str(lastReadDate))
        self.dynaCursor.execute("SELECT * FROM id" + str(self.houseId) + " ORDER BY TimeStamp DESC LIMIT 1")
        lastReadDate = self.dynaCursor.fetchone()[6]
        previousDate = datetime.datetime.strptime(lastReadDate, "%Y-%m-%d %H:%M:%S")
        if previousDate.hour == 9 and previousDate.minute == 59 :
            alteredDate = str(previousDate)
        elif previousDate.hour >= 10 and previousDate.minute >= 0 :
            #This is the case where the last reading was after 10:00AM on that day
            #Delete all rows up until 09:59AM
            alteredDate = previousDate.replace(hour = 10, minute = 0, second = 0)
            self.dynaCursor.execute("DELETE FROM id" + str(self.houseId) + " WHERE TimeStamp IN (SELECT TimeStamp FROM id" 
                                    + str(self.houseId) + " WHERE TimeStamp >= '" + str(alteredDate) + "')")
            self.dynaConn.commit() 
        else :
            #This is the case where the last reading was before 10:00AM
            #Delete all rows up until 09:59AM on the PREVIOUS day
            alteredDate = previousDate.replace(day = previousDate.day - 1, hour = 10, minute = 0, second = 0)
            self.dynaCursor.execute("DELETE FROM id" + str(self.houseId) + " WHERE TimeStamp IN (SELECT TimeStamp FROM id" 
                                    + str(self.houseId) + " WHERE TimeStamp >= '" + str(alteredDate) + "')")
            self.dynaConn.commit() 
        #The excess rows have been removed and now the database can be back-filled until today
        date = str(datetime.date.today())   #today's date
        startDate = datetime.datetime.strptime(str(alteredDate), "%Y-%m-%d %H:%M:%S")
        startDate = startDate.replace(hour = 0, minute = 0, second = 0)
        endDate = datetime.datetime.strptime(date, "%Y-%m-%d") #date object that can return day, month, year
        self.threadClass1 = ThreadClass(startDate, endDate, self.houseId, houseDatabaseString)
        self.threadClass1.start()
        self.threadRunning = True
        self.threadClass1.update_Console.connect(self.receiveConsoleUpdates)
        # dataFrame = retrieveMeterData(1, '04', '10', '2019')
        # dataFrame.to_csv(r'C:\Users\Ryan Phelan\Desktop\ENGG4801\ENGG4801_RyanPhelan\dataFrame.csv', header=True)

    def sendSignalToThread(self, signal):
        if not self.threadRunning : 
            return
        else :
            self.threadClass1.receive_signal.emit(str(signal))
    
    def closeDynamicConnection(self):
        """Stop the currently running thread and close the connection to the dynamic database
        """
        if not self.threadRunning : 
            return
        else :
            self.sendSignalToThread("CLOSE_THREAD")
            self.dlg.columnsListWidget.clear()
            self.dlg.dimensionsListWidget.clear()
            self.dlg.measuresListWidget.clear()
            self.threadRunning = False

    def receiveConsoleUpdates(self, updateString):
        """Receives emitted signals from a thread and send it to the correct console update function.

        Args:
            updateString: the string that will be added to the console
        """
        if int(updateString[0]) == 0 :
            self.addLineToConsole(updateString[1:])
            print("House" in updateString[1:])
            print(self.dlg.autogenerateVisualizationsEveryIntervalCheckbox.isChecked())
            print(self.runAtLeastOnce)
            if ("House" in updateString[1:] and self.dlg.autogenerateVisualizationsEveryIntervalCheckbox.isChecked() and
                    self.runAtLeastOnce) :
                self.outputToPane()
            if updateString[1:] == "Beginning dynamic connection. New data will be appended every interval." :
                dataFrame = initialiseRedbackHouse(1)
                self.dlg.columnsListWidget.addItems(dataFrame.columns) 
                for i in range(0,6) :
                    if i >= 3:
                        self.dlg.columnsListWidget.setCurrentRow(1)
                    else :
                        self.dlg.columnsListWidget.setCurrentRow(0)
                    if i == 5:
                        self.assign_as_dimension()
                    else :
                        self.assign_as_measure()
                self.dlg.columnsListWidget.clearSelection()
                self.dlg.refQueryInput.setText("SELECT * FROM id" + str(self.houseId) + 
                                                " WHERE strftime('%m', TimeStamp) = '08'")
        else :
            self.addRepeatingLineToConsole(updateString[1:])
        
    def addLineToConsole(self, lineString):
        """Method that takes a string and adds it to the makeshift console text-box.

        Args:
            lineString: the line that will be 'printed' to the console.
        """
        self.dlg.myConsole.setText(self.dlg.myConsole.toPlainText() + ">> " + str(lineString) + "\n")
        self.dlg.myConsole.verticalScrollBar().setValue(self.dlg.myConsole.verticalScrollBar().maximum())
        self.dlg.myConsole.repaint()
    
    def addRepeatingLineToConsole(self, lineString):
        """Method removes the previous line from the console and adds lineString in its place.
        THIS METHOD ASSUMES THE CURRENT LAST-STRING IS THE SAME LENGTH AS THE STRING BEING ADDED.
        Args:
            lineString: the line that will be 'printed' to the console.
        """
        currentConsoleTextLength = len(self.dlg.myConsole.toPlainText())
        newTextLength = len(lineString) + 4 #Add the +4 to account for the >> and the new line character
        newConsoleText = self.dlg.myConsole.toPlainText()[0:(currentConsoleTextLength - newTextLength)]
        self.dlg.myConsole.setText(newConsoleText + ">> " + str(lineString) + "\n")
        self.dlg.myConsole.verticalScrollBar().setValue(self.dlg.myConsole.verticalScrollBar().maximum())
        self.dlg.myConsole.repaint()


class ThreadClass(QtCore.QThread):
    update_Console = pyqtSignal(str)
    receive_signal = pyqtSignal(str)
    startDate = None
    endDate = None
    lastDate = None
    secondsDifference = 60
    houseId = None
    connectionString = None
    dynamicConnection = None
    dynamicCursor = None
    loopingData = False

    def __init__(self, startDate, endDate, houseId, connectionString, parent=None):
        super(ThreadClass,self).__init__(parent)
        self.receive_signal[str].connect(self.handleReceivedSignals)
        self.startDate = startDate
        self.endDate = endDate
        self.houseId = houseId
        self.connectionString = connectionString
        self.create_new_connection(self.connectionString)
    
    def handleReceivedSignals(self, command):
        if command == "CLOSE_THREAD" :
            self.closeThread()
        else :
            print("Signal Received: " + str(command))

    def create_new_connection(self, string):
        try:
            self.dynamicConnection = sqlite3.connect(string, check_same_thread=False)   #Set Dynamic Connection
            self.dynamicCursor = self.dynamicConnection.cursor()   #Set Dyanamic Cursor
        except Error as e:
            print(e)        #TODO print errors out to a make shift terminal
    
    def unlockDatabase(self, string):
        self.dynamicConnection = sqlite3.connect(string)
        self.dynamicConnection.commit()
        self.dynamicConnection.close()
        time.sleep(2)

    def run(self):
        self.retrieveDays()
        #self.repeatedlyGetDataToTest()
        dataFrame = retrieveMeterData(1, '04', '10', '2019')
        dataFrame.to_csv(r'C:\Users\Ryan Phelan\Desktop\ENGG4801\ENGG4801_RyanPhelan\dataFrame.csv', header=True)
        self.loopReceivingData()
        self.unlockDatabase(self.connectionString)

    def closeThread(self):
        self.loopingData = False
        self.update_Console.emit("0-----------------")
        self.update_Console.emit("0Connection successfully closed.")
        self.terminate()
        self.quit()

    def repeatedlyGetDataToTest(self):
        count = 1
        while self.loopingData:
            print("Trying to get new data..." + str(count))
            count = count + 1
            newData = retrieveMeterData(self.houseId, '07', '10', '2019')
            now = datetime.datetime.now()   #today's date
            now = now.replace(microsecond = 0)
            theTimeIsNow = datetime.datetime.strptime(str(now), "%Y-%m-%d %H:%M:%S")
            for i in range(-3,0):
                dateOfReading = datetime.datetime.strptime(str(newData['TimeStamp'].iloc[i]), "%Y-%m-%d %H:%M:%S")
                if dateOfReading > self.lastDate :
                    print("New data: " + str(newData['TimeStamp'].iloc[i]) + "  Current time: " + str(theTimeIsNow))
                    count = 1
            self.lastDate = datetime.datetime.strptime(str(newData['TimeStamp'].iloc[-1]), "%Y-%m-%d %H:%M:%S")
            time.sleep(1)

    def loopReceivingData(self):
        self.update_Console.emit("0Beginning dynamic connection. New data will be appended every interval.")
        self.update_Console.emit("0-----------------")
        time.sleep(2)
        self.update_Console.emit("0House " + str(self.houseId) + " data received: " + str(self.lastDate))
        while self.loopingData:
            newData = self.getLatestData()
            if newData is None:
                #no new data was found, wait 5 seconds and try again.
                time.sleep(10)
                newData = self.getLatestData()
                if newData is None:
                    time.sleep(20)
                    newData = self.getLatestData()
                    if newData is None:
                        #That's enough, stop it and just wait another round for the next batch.
                        time.sleep(self.secondsDifference - 30)
                    else:
                        for index, row in newData.iterrows():
                            self.update_Console.emit("0House " + str(self.houseId) + " data received: " + str(row[6]))
                        newData.to_sql("id" + str(self.houseId), self.dynamicConnection, if_exists='append', index=False)
                else:
                    for index, row in newData.iterrows():
                        self.update_Console.emit("0House " + str(self.houseId) + " data received: " + str(row[6]))
                    newData.to_sql("id" + str(self.houseId), self.dynamicConnection, if_exists='append', index=False)
                    time.sleep(self.secondsDifference - 10)
            else:
                #new rows have been found, append them to the database then .
                for index, row in newData.iterrows():
                    self.update_Console.emit("0House " + str(self.houseId) + " data received: " + str(row[6]))
                newData.to_sql("id" + str(self.houseId), self.dynamicConnection, if_exists='append', index=False)
                time.sleep(self.secondsDifference)
            #At the end of this function, update the lastDate to be the last line of the database.
            

    def getLatestData(self):
        """Method retrieves the latest possible data from the server. Only returns a one (or more) lines

        Returns:
            newData: a pandas dataframe containing new data from the server, if no new data, returns none.
        """
        previousDay = self.lastDate - datetime.timedelta(days=1)
        if self.lastDate.hour >= 10 and self.lastDate.minute >= 0 :
            day = self.lastDate.day
            if day <= 10 :
                dayString = '0' + str(day)
            else :
                dayString = str(day)
            if self.lastDate.month <= 10 :
                month = '0' + str(self.lastDate.month)
            else :
                month = str(self.lastDate.month)
            year = str(self.lastDate.year)
        else :
            day = previousDay.day
            if day <= 10 :
                dayString = '0' + str(day)
            else :
                dayString = str(day)
            if previousDay.month <= 10 :
                month = '0' + str(previousDay.month)
            else :
                month = str(previousDay.month)
            year = str(previousDay.year)
        newData = retrieveMeterData(self.houseId, dayString, month, year)
        newDataOnly = pandas.DataFrame()
        #Maybe only loop through the last 5 rows? Kind jib but it would definitely work.
        print("Getting data, last reading: " + str(self.lastDate) + " new reading: " + str(newData['TimeStamp'].iloc[-1]))
        for i in range(-3,0):
            dateOfReading = datetime.datetime.strptime(str(newData['TimeStamp'].iloc[i]), "%Y-%m-%d %H:%M:%S")
            if dateOfReading > self.lastDate :
                print("New data found: " + str(newData['TimeStamp'].iloc[i]))
                newDataOnly = newDataOnly.append(newData.iloc[i])
        if newDataOnly.empty:
            return None #there is no new data, return none and let other function wait to retrieve next data.
        else:
            #This is where we need to check the number of seconds between now and the last reading. It will always be in the past.
            #So this means we just need to get the number of seconds between this and now, subtract another 5, then subtract that from 60.
            self.lastDate = datetime.datetime.strptime(str(newDataOnly['TimeStamp'].iloc[-1]), "%Y-%m-%d %H:%M:%S")
            preferredReadDate = self.lastDate + datetime.timedelta(0,33) #chuck on 33 seconds to ensure it works.
            now = datetime.datetime.now()   #today's date
            now = now.replace(microsecond = 0)
            theTimeIsNow = datetime.datetime.strptime(str(now), "%Y-%m-%d %H:%M:%S")
            self.secondsDifference = 60 - (theTimeIsNow-preferredReadDate).total_seconds()
            if self.secondsDifference < 10 : self.secondsDifference = 10
            print("Last:      " + str(self.lastDate) + "\nPreferred: " + str(preferredReadDate) + 
                    "\nNow:        " + str(theTimeIsNow) + "\nSeconds Difference: " + str(self.secondsDifference))
            return newDataOnly

        
    
    def retrieveDays(self):
        currentDate = self.startDate
        delta = self.endDate - self.startDate
        if delta.days == 0:
            deltaString = '001'
        elif delta.days < 10 :
            deltaString = '00' + str(delta.days)
        elif delta.days < 100 :
            deltaString = '0' + str(delta.days)
        else :
            deltaString = str(delta.days)
        self.update_Console.emit("0Retrieving missing day 001 of " + deltaString)
        currentProgress = 0
        while currentDate <= self.endDate :
            if currentProgress != 0:
                if currentProgress < 10 :
                    currentProgressString = '00' + str(currentProgress)
                elif currentProgress < 100 :
                    currentProgressString = '0' + str(currentProgress)
                else :
                    currentProgressString = str(currentProgress)
                self.update_Console.emit("1Retrieving missing day " + currentProgressString + " of " + deltaString)
            if currentDate.day <= 10 :
                day = '0' + str(currentDate.day)
            else :
                day = str(currentDate.day)
            if currentDate.month <= 10 :
                month = '0' + str(currentDate.month)
            else :
                month = str(currentDate.month)
            year = str(currentDate.year)
            df = retrieveMeterData(self.houseId, day, month, year)
            df.to_sql("id" + str(self.houseId), self.dynamicConnection, if_exists='append', index=False)
            currentDate += datetime.timedelta(days=1)
            currentProgress = currentProgress + 1
        #Get the current 'last line' of the database and use it to set the self.liveDate
        self.dynamicCursor.execute("SELECT * FROM id" + str(self.houseId) + " ORDER BY TimeStamp DESC LIMIT 1")
        self.lastDate = datetime.datetime.strptime(str(self.dynamicCursor.fetchone()[6]), "%Y-%m-%d %H:%M:%S")
        print("lastDate = " + str(self.lastDate))
        self.loopingData = True
        self.update_Console.emit("0Back-fill complete.")

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