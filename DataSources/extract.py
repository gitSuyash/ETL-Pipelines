import pandas as pd
import requests
import json

class Extract:

	def __init__(self):
		self.data_sources = json.load(open('data_config.json'))
		self.api = self.data_sources['data_sources']['api']
		self.csv_path = self.data_sources['data_sources']['csv']

	def getAPISData(self,api_name):

		api_url = self.api[api_name]
		response = requests.get(api_url)
		return response.json()
		
	def getCSVData(self,csv_name):

		df = pd.read_csv(self.csv_path[csv_name])
		# df.to_csv('sample.csv')
		return df

# myclass = Extract()
# myclass.getCSVData('CryptoMarket')
# print('success run')