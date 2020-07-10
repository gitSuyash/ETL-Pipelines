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

			self.data = extractObj.getAPISData(dataSet)
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
		air_df.to_csv('Air Quality.csv')

		

Transformation('api','Pollution')  #test run

