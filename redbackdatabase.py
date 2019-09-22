import pyodbc 

def redBackRequest():
	# Testing the link that Richard sent through
    # conn = pyodbc.connect('DRIVER=ODBC Driver 17 for SQL Server;'
    #         'Server=tcp:rbresearchserver.database.windows.net,1433;'
    #         'Initial Catalog= researchSqlDb;'
    #         'User ID= redbackresearch_admin;'
    #         'Password= e6kUM-6DX}U*)2s%!@;')
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:rbresearchserver.database.windows.net,1433;UID=redbackresearch_admin;PWD=e6kUM-6DX}U*)2s%!@;')
    cursor = conn.cursor()

    for row in cursor.tables():
        print (row.table_name)

redBackRequest()