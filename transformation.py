from DataSources import Extract

import urllib
import pandas as pd
import numpy as np

class Transformation:

	def __init__(self,dataSource,dataSet):

		extractObj = Extract()

		if dataSource == 'api':

			self.data = extractObj.getAPISData(dataSet)
			funcName = dataSource+dataSet
			getattr(self,funcName)()

		elif dataSource == 'csv':

			self.data = extractObj.getCSVData(dataSet)
			funcName = dataSource+dataSet
			getattr(self,funcName)()

		else: print('Unkonwn Data Source !! Try again...')

	def apiPollution(self):

		air_data = self.data['results']
		air_list = []
		for data in air_data:
			for measurement in data['measurements']:
				air_dict = {}
				air_dict['city'] = data['city']
				air_dict['country'] = data['country']
				air_dict['parameter'] = measurement['parameter']
				air_dict['value'] = measurement['value']
				air_dict['unit'] = measurement['unit']

				air_list.append(air_dict)

		air_df = pd.DataFrame(air_list,columns=air_dict.keys())
		# air_df.to_csv('Air Quality.csv')

	def apiEconomy(self):
		economy_data = self.data['records']
		# print(economy_data)
		economy_list=[]
		for record in economy_data:
			economy_dict={}
			economy_dict['financial_year'] = record['financial_year']
			economy_dict['gross_domestic_product'] = record['gross_domestic_product_in_rs_cr_at_2004_05_prices']
			economy_dict['industry_growth_rate'] = record['industry___growth_rate_yoy_']
			economy_dict['services_growth_rate'] = record['services___growth_rate_yoy_']

			economy_list.append(economy_dict)
		economy_df = pd.DataFrame(economy_list,columns = economy_dict.keys())
		economy_df.to_csv('Economy data.csv')

	def csvCryptoMarket(self):

		self.data['open'] = self.data['open'].apply(lambda x:x*0.75)
		self.data['low'] = self.data['low'].apply(lambda x:x*0.75)
		self.data['high'] = self.data['high'].apply(lambda x:x*0.75)
		self.data['close'] = self.data['close'].apply(lambda x:x*0.75)

		self.data.to_csv('Crypto.csv')

Transformation('csv','CryptoMarket')
# Transformation('api','Economy')
# Transformation('api','Pollution')  #test run

