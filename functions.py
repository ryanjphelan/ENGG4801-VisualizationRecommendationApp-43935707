import sys
import pandas

def generateQueriesAsViewTuples(d, f, m, Dq, Dr):
    """Function that returns all possible queries required for generating recommendations.
    The number of combinations returned follows the formula --> 2 x f x d x m.

    Args:
        d: list of dimension attributes.
        f: list of aggregate functions.
        m: list of measure attributes.
        Dq: name of the user-defined query database subset.
        Dr: name of the reference database.

    Returns:
        A list of tuples, each tuple represents a view: (Q(Dq), Q(Dr)).
        A view consists of the same query but only changing which database it's executed on.
    """
    generatedQueries = []
    for i in range(len(d)):
        for j in range(len(f)):
            for k in range(len(m)):
                # Create the queries as strings
                targetView = "SELECT \"" + str(d[i]) + "\", " + str(f[j]) + "(\"" + \
                    str(m[k]) + "\") FROM " + str(Dq) + " GROUP BY \"" + str(d[i]) + "\""
                referenceView = "SELECT \"" + str(d[i]) + "\", " + str(f[j]) + "(\"" + \
                    str(m[k]) + "\") FROM " + str(Dr) + " GROUP BY \"" + str(d[i]) + "\""
                view = (targetView, referenceView)
                generatedQueries.append(view)
    # Return the list of views
    return generatedQueries
                
def generateQueriesAsStrings(d, f, m, Dq, Dr):
    """Function that returns all possible queries required for generating recommendations.
    The number of combinations returned follows the formula --> 2 x f x d x m.

    Args:
        d: list of dimension attributes.
        f: list of aggregate functions.
        m: list of measure attributes.
        Dq: name of the user-defined query database subset.
        Dr: name of the reference database.

    Returns:
        A list of strings, each string is a single SQL query.
    """
    generatedQueries = []
    for i in range(len(d)):
        for j in range(len(f)):
            for k in range(len(m)):
                # Create the queries as strings
                targetView = "SELECT \"" + str(d[i]) + "\", " + str(f[j]) + "(\"" + \
                    str(m[k]) + "\") FROM " + str(Dq) + " GROUP BY \"" + str(d[i]) + "\""
                referenceView = "SELECT \"" + str(d[i]) + "\", " + str(f[j]) + "(\"" + \
                    str(m[k]) + "\") FROM " + str(Dr) + " GROUP BY \"" + str(d[i]) + "\""
                generatedQueries.append(targetView)
                generatedQueries.append(referenceView)
    # Return the list of views
    return generatedQueries

def generateQueryRecommendations(dataFrame, dimensionAttributes, measureAttributes):
    """Function that returns a list of query recommendations to the user.
    NOTE: This function assumes the underlying dataframe is called 'dataBase'

    Args:
        dataFrame: a pandas dataframe
        dimensionAttributes: list of dimension attributes
        measureAttributes: list of measure attributes

    Return:
        A list of strings, each representing a different query on the dataDase
    """
    generatedQueryRecommendations = []
    for dim in dimensionAttributes:
        dimValues = dataFrame[dim].unique()
        for value in dimValues:
            value = value.replace("'", "''")
            query = '''SELECT * FROM dataBase WHERE [''' + dim + "] = " + "'" + value + "'"
            generatedQueryRecommendations.append(query)

    return generatedQueryRecommendations
    



