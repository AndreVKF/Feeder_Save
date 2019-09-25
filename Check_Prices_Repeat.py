from SQL_Server_Connector import SQL_Server_Connection
from Queries import Queries
from Indexes import Indexes
from Functions import *
from API_BBG import *

from datetime import datetime

import sys
import pandas as pd
import numpy as np

# Main
################## Rotinas para cuidar de precos ausentes ##################
if __name__ == '__main__':
    # Main classes
    # now = datetime.now()
    Queries = Queries()
    SQL_Server_Connection = SQL_Server_Connection(database='PM')

    # Refdate = 20190423
    Refdate = int(sys.argv[1])
    Log_DirSrc = sys.argv[2]

    # Abre log
    file = open(Log_DirSrc, "a+")

    # Time now
    Time_Now = datetime.now().strftime("%Y%m%d %H:%M:%S")

    # Yd_Date
    Yd_Date = SQL_Server_Connection.getValue(query=Queries.getYd_Date(Refdate=Refdate))

    ############# Checa precos ausentes em Refdate #############

    # Missing price DF
    query = Queries.getPos_PriceMiss(refdate=Refdate)
    Missing_Px = SQL_Server_Connection.getData(query=query)

    if not Missing_Px.empty:

        ################ Pega precos da tabela prices de D-1 e repete ################
        # Products list
        Id_Products = str(Missing_Px['Id_Product'].tolist())
        Id_Products = str.replace(Id_Products, '[', '')
        Id_Products = str.replace(Id_Products, ']', '')

        # Id_Products = "2384, 2383, 2381"
        Yd_query = Queries.getPrices_byProdId(Refdate=Yd_Date, Id_Products=Id_Products)
        Yd_Px = SQL_Server_Connection.getData(query=Yd_query)

        # Monta DF de precos que faltam
        Prices_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': Yd_Px['Id_Product'],
            'Price': Yd_Px['Price'],
            'Value': Yd_Px['Value'],
            'DV01': Yd_Px['DV01'],
            'Delta1$': Yd_Px['Delta1$'],
            'Delta%': Yd_Px['Delta%'],
            'Gamma$': Yd_Px['Gamma$'],
            'Vega$': Yd_Px['Vega$'],
            'Theta$': Yd_Px['Theta$'],
            'Delta2$': Yd_Px['Delta2$'],
            'Ivol': Yd_Px['Ivol'],
            'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Delete history
        Delete_Hist_Asset = Delete_Price_Hist(
            Refdate=Refdate, Price_Df=Yd_Px, SQL_Server_Connection=SQL_Server_Connection, Queries=Queries)

        # Insert Price_DF to database
        SQL_Server_Connection.insertDataFrame(tableDB='Prices', df=Prices_DF)

        # File Log
        file.write(f"Refdate:{Refdate} Precos Repetidos de D-1:{str(Yd_Px['Name'].tolist())} Time:{Time_Now}\n")

    ############# Check Entities quotes prices que nao foram atualizadas em Refdate #############
    query = Queries.getMissing_EntQuotesPx(Refdate=Refdate, Yd_Date=Yd_Date)
    EntQuotes_Px = SQL_Server_Connection.getData(query=query)

    # Se existerem precos a serem repetidos em Refdate
    if not EntQuotes_Px.empty:
        # Monta DF de precos que faltam
        Prices_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': EntQuotes_Px['Id_Product'],
            'Price': EntQuotes_Px['Yd_Px'],
            'Value': EntQuotes_Px['Yd_Value'],
            'DV01': np.nan,
            'Delta1$': EntQuotes_Px['Delta1$'],
            'Delta%': np.nan,
            'Gamma$': np.nan,
            'Vega$': np.nan,
            'Theta$': np.nan,
            'Delta2$': EntQuotes_Px['Delta2$'],
            'Ivol': np.nan,
            'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Delete history
        Delete_Hist_Asset = Delete_Price_Hist(
            Refdate=Refdate, Price_Df=Prices_DF, SQL_Server_Connection=SQL_Server_Connection, Queries=Queries)

        # Insert Price_DF to database
        SQL_Server_Connection.insertDataFrame(tableDB='Prices', df=Prices_DF)

        # File Log
        file.write(f"Refdate:{Refdate} Repetindos Quotes:{str(EntQuotes_Px['Name'].tolist())} Time:{Time_Now}\n")

    file.close()
