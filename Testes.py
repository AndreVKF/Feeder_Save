import sys
import requests
import json
import pandas as pd
import numpy as np

from pandas.io.json import json_normalize
from API_BBG import *

from datetime import date, datetime
from time import gmtime, strftime
from bizdays import Calendar, load_holidays

from SQL_Server_Connector import SQL_Server_Connection
from Queries import Queries
from Functions import *
from API_BBG import *

from pandas.io.json import json_normalize
from datetime import datetime

PM_BBG = BBG_POST(bbg_request='BDH', tickers=['XS2060698219 Corp'],
                      fields=['RISK_MID'], date_start=20200106,
                      date_end=20200106)


 # Formata DF
PM_DF = (BBG_Json_to_DF(Request_type='BDH',
                        BBG_response=PM_BBG)).reset_index()

PM_DF.rename(columns={'index': 'BBGTicker'}, inplace=True)
PM_BBGPriceDF = pd.merge(PM_DF, PM_Products,
                            on='BBGTicker', how='left')


# Calendario
Calendar_DF =SQL_Server_Connection.getData(query='SELECT * FROM Feriados_USA ORDER BY Data')
Calendar_List = Calendar_DF['Data'].tolist()

Calendar_USA = Calendar(Calendar_List, ['Sunday', 'Saturday'], name='T+2')

Calendar_USA.offset('2019-12-31', 2)

Queries = Queries()
SQL_Server_Connection = SQL_Server_Connection(database='PM')

url = "http://10.1.1.31:8099/App_BBG_Request/BDP/"
data_post = {
     "tickers": ['US31572UAF30@BGN Corp', 'US71647NAN93@BGN Corp'],
    "fields": ['YAS_RISK'],
    "overrides": overrides
}

r = requests.post(url = url, data = json.dumps(data_post))
r.json()

query = '''
SELECT 
	Date
	,Products.BBGTicker
	,DV01
FROM 
	Prices 
LEFT JOIN Products ON Prices.Id_Product=Products.Id
WHERE 
	Products.Id_Instrument IN (44, 45)
	AND Date>'20190701'
	AND DV01 IS NOT NULL
'''


