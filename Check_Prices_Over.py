from SQL_Server_Connector import SQL_Server_Connection
from Queries import Queries
from Functions import *

from datetime import datetime

import sys
import pandas as pd
import numpy as np

# Main
if __name__ == '__main__':
    # Main classes
    Queries = Queries()
    SQL_Server_Connection = SQL_Server_Connection(database='PM')

    # Refdate = 20190502
    Refdate = int(sys.argv[1])
    Log_DirSrc = sys.argv[2]

    # Time now
    Time_Now = datetime.now().strftime("%Y%m%d %H:%M:%S")

    # Missing price DF
    query = Queries.getPrices_Override(Refdate=Refdate)
    Override_Px = SQL_Server_Connection.getData(query=query)

    ################## Rotina para dar override nos precos dos ativos presentes na tabela Prices_Over ##################
    if not Override_Px.empty:
        # Abre log
        file = open(Log_DirSrc, "a+")

        ################ Override Prices ################
        # Products list
        Id_Products = str(Override_Px['Id_Product'].tolist())
        Id_Products = str.replace(Id_Products, '[', '')
        Id_Products = str.replace(Id_Products, ']', '')

        # Get Products description
        query = Queries.getProducts_byId(Id_Products=Id_Products)
        Products = SQL_Server_Connection.getData(query=query)

        # Monta DF de precos que faltam
        Prices_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': Override_Px['Id_Product'],
            'Price': Override_Px['Price'],
            'Value': Override_Px['Value'],
            'DV01': Override_Px['DV01'],
            'Delta1$': Override_Px['Delta1$'],
            'Delta%': Override_Px['Delta%'],
            'Gamma$': Override_Px['Gamma$'],
            'Vega$': Override_Px['Vega$'],
            'Theta$': Override_Px['Theta$'],
            'Delta2$': Override_Px['Delta2$'],
            'Ivol': Override_Px['Ivol'],
            'Last_Update': strftime("%Y-%m-%d %H:%M:%S", gmtime())
        })

        # Delete history
        Delete_Hist_Asset = Delete_Price_Hist(
            Refdate=Refdate, Price_Df=Override_Px, SQL_Server_Connection=SQL_Server_Connection, Queries=Queries)

        # Insert Price_DF to database
        SQL_Server_Connection.insertDataFrame(tableDB='Prices', df=Prices_DF)

        # Px override log
        # File Log
        file.write(f"Refdate:{Refdate} Precos Sobreescrito:{str(Products['Name'].tolist())} Time:{Time_Now}\n")

        file.close()
