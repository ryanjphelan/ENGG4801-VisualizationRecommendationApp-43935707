import sys
import os
import datetime
import time
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
        self.dynamicDataBase = None
        self.preview = None
        self.recommendations = None
        self.dimensions = None
        self.measures = None
        self.conn1 = None
        self.conn2 = None
        self.dynaConn = None
        self.cur1 = None
        self.cur2 = None
        self.dynaCursor = None
        self.toolBar = None
        self.fileName = None
        self.query = None
        self.threadClass1 = None
        self.threadRunning = False
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
        self.dlg.generateRecommendationsButton.clicked.connect(self.generateRecommendations)
        self.dlg.executeRefQueryButton.clicked.connect(self.execute_ref_query)
        self.dlg.previewRefQueryButton.clicked.connect(self.preview_ref_query)
        self.dlg.generateVisualizationsButton.clicked.connect(self.outputToPane)
        self.dlg.connectToHouseButton.clicked.connect(self.connectToHouse)
        self.dlg.closeDynamicConnectionButton.clicked.connect(self.closeDynamicConnection)

    def browse_for_file(self):
        """Open a windows File-Dialog to allow the user to simultaneously define the filepath and the filename
        """
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
            self.cur2 = self.conn1.cursor()         #Set Cursor 2
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

    def import_file(self):
        """Import csv, Retrieve column names, create the connection, convert database to SQL.
        """
        filePath = self.dlg.fileLocation.text()      #Select the file path directly from the textbox
        if not filePath : return                     #Check if not null
        self.dataBase = pandas.read_csv(filePath, encoding = "ISO-8859-1") #Create the pandas dataframe using .read_csv
        self.dlg.columnsListWidget.addItems(self.dataBase.columns)  #Add the column names to the columnsListWidget
        self.create_connection('databases/dataBaseRef.db')     #Create the connection to dataBaseRef.db
        self.dataBase.to_sql("dataBaseRef", self.conn1, if_exists='replace', index=False) #Convert to SQL database
        

    def preview_file(self):
        """Load the database into a temporary widget to let the user 'preview' their data and ensure its correctness.
        Exit function if they haven't defined a filepath.
        """
        if self.dataBase is None : return
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
        self.queryText = query
        print(query)
        self.dataBaseSub = pandas.read_sql_query(query, self.conn1) #Read query using pandas method
        self.create_connection2('databases/dataBaseSub.db')                   #Create Connected 2
        self.dataBaseSub.to_sql("dataBaseSub", self.conn2, if_exists='replace', index=False) #Make database SQL

    def preview_ref_query(self):
        """Load dataBaseSub into a temporary widget and'preview' the data in a pop-up widget.
        Exit function if dataBaseSub isn't defined.
        """
        if self.dataBaseSub is None : return
        model = PandasModel(self.dataBaseSub.head(100))   #Create a model of the database, only get TOP 100 rows
        self.preview = QtWidgets.QWidget()      #Create a TableView for the UI
        ui = TableView()                        #Create the 'preview' widget
        ui.setupUi(self.preview, model)         #Load the model into the created widget
        self.preview.show()                     #Show the widget       
        self.preview.setWindowTitle("SELECT TOP 100 FROM Reference DataSet")   

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
        #Loop through and calculate the difference of the two scores squared
        for index, row in combinedDataFrame.iterrows():
            total = total + (row[combinedDataFrame.columns[1]] - row[combinedDataFrame.columns[2]])**2
        #Return the utlity by taking the square root of the total
        return math.sqrt(total)

    def getKey(self, item):
        return item[1]

    def calculate_all_utilities(self, queriesTuple):
        print("Starting : calculate_all_utilities")
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
            title = queryTar + '\n' +queryRef
            view = (title, combined, utility)
            print(queryTar + '\n' +queryRef)
            #print(view)
            allViews.append(view)

        sortedViews = sorted(allViews, key = lambda x: x[2])
        print("Ended : calculate_all_utilities")
        return sortedViews
        
    def plot_visualizations(self, allViews):
        print("Starting : plot_visualizations")
        plt.clf()
        k = int(str(self.dlg.kValueBox.currentText()))
        print(k)
        self.dlg.MplWidget.canvas.figure.clf()
        ax = self.dlg.MplWidget.setAxes(1) #set the axes to have a single subplot.
        #self.dlg.MplWidget.canvas.axes.clear()
        ax.title.set_size(40)
        allViews[0][1].plot.bar(rot=0, ax=ax, title=allViews[0][0])
        plt.axes().xaxis.set_label_text("HI THERE THIS WORKS?")
        self.dlg.MplWidget.canvas.draw()
        print("Ended : plot_visualizations")


    def outputToPane(self):
        print("Starting : outputToPane")
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
        #Checks and balances to ensure that the queries should be generated and not error
        if not aggregateFunctions or not self.dimensions or not self.measures :
            return
        queries = generateQueriesAsViewTuples(self.dimensions, aggregateFunctions, self.measures, "dataBaseSub", "dataBaseRef")
        allViews = self.calculate_all_utilities(queries)
        self.plot_visualizations(allViews)
        print("Ended : outputToPane")
        #self . MplWidget . canvas . axes . legend (( 'cosinus' ,  'sinus' ),loc = 'upper right' ) 
        #self . MplWidget . canvas . axes . set_title ( ' Cosinus - Sinus Signal' ) 
        #self.dlg.visualsPane.addItems(ax)
        #for view in queries:
        #    print (view)

    def connectToHouse(self):
        if self.threadRunning : 
            self.addLineToConsole("Connection is already running. Press 'Close Connection' before changing databases.")
            return
        #TODO NEED TO CHECK THAT YOU HAVE A VALID INTERNET CONNECTION, THROW EXCEPTION
        idString = self.dlg.houseidComboBox.currentText()   #get currently selected house ID
        houseId = int(idString.split()[2])  #convert id string to an integer, split string to get at number
        self.addLineToConsole("Connecting to House " + str(houseId) + "...")
        #Check that the databases folder exists, if not, create it
        if not os.path.isdir('./databases') :
            os.mkdir("./databases")
        #Connect to the dynamic dataset, the name of the database will be based on the house ID
        houseDatabaseString = "databases/id" + str(houseId) + ".db"
        self.create_dynamic_connection(houseDatabaseString)
        #If the database has no tables initialised in it, initialise the first day for the given ID 
        self.dynaCursor.execute('SELECT name from sqlite_master where type= "table"')
        if self.dynaCursor.fetchall() == [] :
            self.addLineToConsole("Initializing House " + str(houseId) + " table in database...")
            self.dynamicDataBase = initialiseRedbackHouse(houseId)
            self.dynamicDataBase.to_sql("id" + str(houseId), self.dynaConn, if_exists='fail', index=False)   
            self.dynaConn.commit()  
        self.dynaCursor.execute("SELECT * FROM id" + str(houseId) + " ORDER BY TimeStamp DESC LIMIT 1")
        lastReadDate = self.dynaCursor.fetchone()[6]
        self.addLineToConsole("LAST KNOWN METER READING: " + str(lastReadDate))
        self.dynaCursor.execute("SELECT * FROM id" + str(houseId) + " ORDER BY TimeStamp DESC LIMIT 1")
        lastReadDate = self.dynaCursor.fetchone()[6]
        previousDate = datetime.datetime.strptime(lastReadDate, "%Y-%m-%d %H:%M:%S")
        if previousDate.hour == 9 and previousDate.minute == 59 :
            alteredDate = str(previousDate)
        elif previousDate.hour >= 10 and previousDate.minute >= 0 :
            #This is the case where the last reading was after 10:00AM on that day
            #Delete all rows up until 09:59AM
            alteredDate = previousDate.replace(hour = 10, minute = 0, second = 0)
            self.dynaCursor.execute("DELETE FROM id" + str(houseId) + " WHERE TimeStamp IN (SELECT TimeStamp FROM id" 
                                    + str(houseId) + " WHERE TimeStamp >= '" + str(alteredDate) + "')")
            self.dynaConn.commit() 
        else :
            #This is the case where the last reading was before 10:00AM
            #Delete all rows up until 09:59AM on the PREVIOUS day
            alteredDate = previousDate.replace(day = previousDate.day - 1, hour = 10, minute = 0, second = 0)
            self.dynaCursor.execute("DELETE FROM id" + str(houseId) + " WHERE TimeStamp IN (SELECT TimeStamp FROM id" 
                                    + str(houseId) + " WHERE TimeStamp >= '" + str(alteredDate) + "')")
            self.dynaConn.commit() 
        #The excess rows have been removed and now the database can be back-filled until today
        date = str(datetime.date.today())   #today's date
        startDate = datetime.datetime.strptime(str(alteredDate), "%Y-%m-%d %H:%M:%S")
        startDate = startDate.replace(hour = 0, minute = 0, second = 0)
        endDate = datetime.datetime.strptime(date, "%Y-%m-%d") #date object that can return day, month, year
        #self.backFillHouseData(startDate, endDate, houseId)
        self.threadClass1 = ThreadClass(startDate, endDate, houseId, houseDatabaseString)
        self.threadClass1.start()
        self.threadRunning = True
        self.threadClass1.update_Console.connect(self.receiveConsoleUpdates)

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
            self.threadRunning = False

    def receiveConsoleUpdates(self, updateString):
        """Receives emitted signals from a thread and send it to the correct console update function.

        Args:
            updateString: the string that will be added to the console
        """
        if int(updateString[0]) == 0 :
            self.addLineToConsole(updateString[1:])
        else :
            self.addRepeatingLineToConsole(updateString[1:])

    def backFillHouseData(self, startDate, endDate, houseId):
        """Method that will continually request data from the RedBack server to append to the local database.
        This method assumes that the current last-row in the database is 09:59AM on the day of startDate.

        Args:
            startDate: the last known date for this database.
            endDate: today, where we want to back-fill up-to.
            houseId: the house number
        """
        dataFrame = retrieveMeterData(1, '04', '10', '2019')
        dataFrame.to_csv(r'C:\Users\Ryan Phelan\Desktop\ENGG4801\ENGG4801_RyanPhelan\dataFrame.csv', header=True)


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
        self.loopReceivingData()
        self.unlockDatabase(self.connectionString)

    def closeThread(self):
        self.loopingData = False
        self.update_Console.emit("0-----------------")
        self.update_Console.emit("0Connection successfully closed.")
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