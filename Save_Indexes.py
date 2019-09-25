from SQL_Server_Connector import SQL_Server_Connection
from Queries import Queries
from Indexes import Indexes
from Functions import *
from API_BBG import *

from datetime import datetime

import sys
import pandas as pd
import numpy as np

# Main Script
if __name__ == '__main__':
    # Set Refdate as Integer
    # Refdate = 20190425
    Refdate = int(sys.argv[1])
    Log_DirSrc = sys.argv[2]

    TdDate = int(date.today().strftime("%Y%m%d"))
    # Set Request type [BDP, BDH]
    BBG_Req = 'BDH'
    if Refdate == TdDate:
        BBG_Req = 'BDP'

    # Abre log
    file = open(Log_DirSrc, "a+")

    # Time now
    Time_Now = datetime.now().strftime("%Y%m%d %H:%M:%S")

    Queries = Queries()
    SQL_Server_Connection = SQL_Server_Connection(database='PM')

    ######################################## Save Indexes ########################################
    Indexes = Indexes(SQL_Server_Connector=SQL_Server_Connection, Queries=Queries)

    Indexes_BBG = BBG_POST(bbg_request='BDH', tickers=Indexes.DF_Index['RiskTicker'].tolist(),
                           fields=['PX_LAST'], date_start=Refdate,
                           date_end=Refdate)

    # Get prices indexes
    Indexes_Prices = Indexes.Index_Prices(Refdate=Refdate, BBG_Req=BBG_Req)

    # Delete history
    Indexes.Index_Delete(Refdate=Refdate, Indexes_Prices=Indexes_Prices)

    # Save indexes
    Indexes.Index_Save(Refdate=Refdate, Indexes_Prices=Indexes_Prices)

    # Log Saved Assets
    file.write(f"Refdate:{Refdate} Saved_Indexes Time:{Time_Now}\n")
    file.close()
