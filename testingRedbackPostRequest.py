import requests
import pandas as pd
import urllib
import io

def redBackRequest():
	"""
	Below is a method for getting at query parameters.
	So essentially it does the same thing at Postman does.
	It splits to url, then it parses just the query component.
	This now gives proper access to the url's query parameters.
	A next step will be recombining this to 
	"""
	# url = urllib.parse.urlsplit('https://ouijalitedatarequest.azurewebsites.net/api/DataCacheRequest?day=08&month=09&year=2019&housenumber=1&code=hQ1gF6gA0Bo5zn/i4NkBWutZ/3nA0a5LUFAHUFNLKD8f5IV2Uac4FA==')
	# print(url)
	# b = urllib.parse.parse_qs(url.query)
	# print(b)

	# #NASA API BELOW:
	# nasaURL = "https://api.nasa.gov/planetary/apod?api_key=JV9otIoDDve0GeK4CUvlwmg2dvFVg5C5Ze9iG8dP"
	# n = urllib.parse.urlsplit(nasaURL)
	# c = urllib.parse.parse_qs(n.query)
	# print(c)
	#OKAY, FROM THE TOP NOW. DO A GET REQUEST TO GET THE DATA FROM THE SERVER
	
	# day = 1
	# dayString = '0' + str(day)
	# print(dayString)
	# params = {'day':dayString,'month':'08','year':'2019','housenumber':'2',
	# 			'code':'hQ1gF6gA0Bo5zn/i4NkBWutZ/3nA0a5LUFAHUFNLKD8f5IV2Uac4FA=='}
	# r = requests.get(url,params=params)
	#print(r.json())
	#df = pd.read_csv(io.StringIO(r.content.decode('utf-8')))
	#print(df)
	'''Method below gets 1 month's worth of data'''
	url = "https://ouijalitedatarequest.azurewebsites.net/api/DataCacheRequest?"
	df = pd.DataFrame() #new blank dataframe

	for day in range(1,32) :
		if day <= 10 :
			dayString = '0' + str(day)
		else :
			dayString = str(day)
		print("Getting: " + dayString + "/08/2019")
		params = {'day':dayString,'month':'08','year':'2019','housenumber':'2',
				'code':'hQ1gF6gA0Bo5zn/i4NkBWutZ/3nA0a5LUFAHUFNLKD8f5IV2Uac4FA=='}
		r = requests.get(url,params=params)
		j = r.json()
		newdf = pd.DataFrame.from_dict(j)
		if day == 1 :
			df = pd.DataFrame(newdf)
		else :
			df = df.append(newdf, ignore_index=True)
		print("Dataframe size = " + str(df.shape[0]))

	csv = df.to_csv(r'C:\Users\Ryan Phelan\Desktop\ENGG4801\ENGG4801_RyanPhelan\House2.csv', 
					index = None, header=True)
		
redBackRequest()



