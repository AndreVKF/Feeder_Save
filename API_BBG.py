import requests
import json
import pandas as pd
from pandas.io.json import json_normalize


# Function para realizar pedidos de post da Aplicação BBG
# BBG_Request = ["BDP", "BDH"]
# Ticker = List<Tickers>
# Fields = Field
def BBG_POST(bbg_request, tickers, fields, date_start=None, date_end=None):

    # Variaveis BDP ou BDH
    if bbg_request == "BDP":
        url = "http://10.1.1.31:8099/App_BBG_Request/BDP/"
        data_post = {
            "tickers": tickers,
            "fields": fields
        }

    elif bbg_request == "BDH":
        url = "http://10.1.1.31:8099/App_BBG_Request/BDH/"
        data_post = {
            "tickers": tickers,
            "fields": fields,
            "date_start": date_start,
            "date_end": date_end
        }

    # Make Requests
    r = requests.post(url=url, data=json.dumps(data_post))

    return r


def BBG_POST_Tst(bbg_request, tickers, fields, date_start=None, date_end=None, overrides=None):
    
    # Variaveis BDP ou BDH
    if bbg_request == "BDP":
        url = "http://10.1.1.31:8099/App_BBG_Request/BDP/"
        data_post = {
            "tickers": tickers,
            "fields": fields,
            "overrides": overrides
        }

    elif bbg_request == "BDH":
        url = "http://10.1.1.31:8099/App_BBG_Request/BDH/"
        data_post = {
            "tickers": tickers,
            "fields": fields,
            "date_start": date_start,
            "date_end": date_end,
            "overrides": overrides
        }

    # Make Requests
    r = requests.post(url=url, data=json.dumps(data_post))
  

    return r

   #  PM_BBG = BBG_POST(bbg_request='BDH', tickers=PM_Products['BBGTicker'].tolist(),
   #                fields=fields, date_start=20190417,
   #                date_end=20190417)
   #
   # PM_BBG.json()
   # PM_BBG.json()
   #
   #    fields=['PX_LAST']
   #    fields=['PX_LAST', 'CONTRACT_VALUE', 'FUT_TICK_VAL', 'FUT_CONT_SIZE']
   #    PM_Products['BBGTicker'].tolist()
   #    tickers = ['USP01014AA03@BGN Corp', 'USP0607LAC74@BGN Corp', 'US02364WBH79@BGN Corp']
   #    tickers = ["USD/BRL @041119  Curncy"]
   #    PM_Products['BBGTicker'].tolist()
   #    type(tickers)
