from SQL_Server_Connector import SQL_Server_Connection
from Queries import Queries
from Functions import *
from API_BBG import *

from datetime import datetime

import sys
import pandas as pd
import numpy as np


# Main Script
if __name__ == '__main__':
    # Set Refdate as Integer
    # Refdate = 20190905
    Refdate = int(sys.argv[1])
    Log_DirSrc = sys.argv[2]

    # Abre log
    file = open(Log_DirSrc, "a+")

    TdDate = int(date.today().strftime("%Y%m%d"))
    # Set Request type [BDP, BDH]
    BBG_Req = 'BDH'
    if Refdate == TdDate:
        BBG_Req = 'BDP'

    # Variaveis auxiliares e inicia conexão com a DB PM
    Assets = ['Equity',
              'Bond',
              'FX Spot',
              'FX Forwards',
              'Listed Opt',
              'Futures',
              'Funds']
    Time_Now = datetime.now().strftime("%Y%m%d %H:%M:%S")

    Queries = Queries()
    SQL_Server_Connection = SQL_Server_Connection(database='PM')

    ######################################## Loop Through Assets ########################################
    # Asset = 'Futures'
    # AssetGroup = 'Futures'
    # Refdate = 20190905
    for Asset in Assets:
        try:
            # Cria DF a ser inserida no banco de dados
            Price_Df = Create_PriceDF(BBG_Req=BBG_Req, Refdate=Refdate,
                                      AssetGroup=Asset, SQL_Server_Connection=SQL_Server_Connection, Queries=Queries)

            # Delete History
            Delete_Hist_Asset = Delete_Price_Hist(
                Refdate=Refdate, Price_Df=Price_Df, SQL_Server_Connection=SQL_Server_Connection, Queries=Queries)

            # Insert Price_DF to database
            SQL_Server_Connection.insertDataFrame(tableDB='Prices', df=Price_Df)

            # Log Saved Assets
            file.write(f"Refdate:{Refdate} Saved_Assets:{Asset} Time:{Time_Now}\n")

        except:
            file.write(f"Refdate:{Refdate} Failed to Saved_Assets:{Asset} Time:{Time_Now}\n")

    ######################################## Assets c/ Preco Um ########################################
    # Cria DF
    PrecoUm_Df = Create_PriceUmDF(
        Refdate=Refdate, SQL_Server_Connection=SQL_Server_Connection, Queries=Queries)

    # Delete History
    Delete_Hist_Asset = Delete_Price_Hist(
        Refdate=Refdate, Price_Df=PrecoUm_Df, SQL_Server_Connection=SQL_Server_Connection, Queries=Queries)

    # Insert Price_DF to database
    SQL_Server_Connection.insertDataFrame(tableDB='Prices', df=PrecoUm_Df)

    # Log Saved Assets
    file.write(f"Refdate:{Refdate} Saved_Assets:Price 1 Assets Time:{Time_Now}\n")

    ######################################## CRAs ########################################
    CRA_Calculator = CRA_Calculator(
        Refdate=Refdate, SQL_Server_Connection=SQL_Server_Connection, Queries=Queries)

    PrecoCRA_Df = Create_CRAPriceDF(
        Refdate=Refdate, SQL_Server_Connection=SQL_Server_Connection, Queries=Queries, CRA_Calculator=CRA_Calculator)

    # Delete History
    Delete_Hist_Asset = Delete_Price_Hist(
        Refdate=Refdate, Price_Df=PrecoCRA_Df, SQL_Server_Connection=SQL_Server_Connection, Queries=Queries)

    # Insert Price_DF to database
    SQL_Server_Connection.insertDataFrame(tableDB='Prices', df=PrecoCRA_Df)

    # Log Saved Assets
    file.write(f"Refdate:{Refdate} Saved_Assets:CRAs Time:{Time_Now}\n")

    ######################################## ########################################

    file.close()
###################################################### END ######################################################
