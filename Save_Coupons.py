from SQL_Server_Connector import SQL_Server_Connection
from Queries import Queries
from Indexes import Indexes
from Bond_Coupons import Bond_Coupons
from Functions import *
from API_BBG import *

from datetime import datetime

import sys
import pandas as pd
import numpy as np


# Main Script
if __name__ == '__main__':
    # Set Refdate as Integer
    # Refdate = 20190918
    Refdate = int(sys.argv[1])
    Log_DirSrc = sys.argv[2]

    TdDate = int(date.today().strftime("%Y%m%d"))
    Refdate == TdDate
    # Set Request type [BDP, BDH]
    BBG_Req = 'BDP'

    # Abre log
    file = open(Log_DirSrc, "a+")

    # Time now
    Time_Now = datetime.now().strftime("%Y%m%d %H:%M:%S")

    ######################################## Save Coupons ########################################
    # Passar para arquivo main qndo estiver pronto
    # PREV_CPN_DT só funciona com BDP
    if Refdate == TdDate:
        Queries = Queries()
        SQL_Server_Connection = SQL_Server_Connection(database='PM')
        Bond_Coupons = Bond_Coupons(SQL_Server_Connection=SQL_Server_Connection, Queries=Queries)

        # Get list of Bonds
        Bonds_DF = Bond_Coupons.getBond_List(Refdate=Refdate)

        ############################### Bond Ratings ###############################
        # Bloomberg Rating
        Bonds_BBG_Ratings = Bond_Coupons.getBond_Ratings(Bonds_DF=Bonds_DF, Refdate=Refdate)

        # Rating Octante
        Bonds_Oct_Ratings = Bond_Coupons.getRatings_Group(Bonds_BBG_Ratings=Bonds_BBG_Ratings)

        # Update BondsRatings
        # Remove History
        Bond_Coupons.deleteBonds_Ratings(Bonds_Oct_Ratings=Bonds_Oct_Ratings, Refdate=Refdate)
        # Insert
        Bond_Coupons.insertBond_Ratings(Bonds_Oct_Ratings=Bonds_Oct_Ratings)
        file.write(f"Refdate:{Refdate} Saved Bonds Ratings Time:{Time_Now}\n")

        ############################### Bond Coupons ###############################
        # Coupon List to Insert
        Bonds_TdCp = Bond_Coupons.getBond_Coupons(Bonds_DF=Bonds_DF, Refdate=Refdate)

        # Asset Events DataFrame
        AssetEvents_DF = Bond_Coupons.createAssetEvent_CouponsDF(
            Bonds_DF=Bonds_DF, Bonds_TdCp=Bonds_TdCp, Refdate=Refdate)

        # Update Asset Events
        # Remove History
        Bond_Coupons.deleteAssetEvents_Coupons(AssetEvents_DF=AssetEvents_DF, Refdate=Refdate)
        # Insert
        Bond_Coupons.insertAssetEvents_Coupons(AssetEvents_DF=AssetEvents_DF)

        # Log Saved Assets
        if not Bonds_TdCp.empty:
            TdCp_List = Bonds_TdCp["BBGTicker"].tolist()
            file.write(f"Refdate:{Refdate} Inserted Coupons Payments for Refdate {Refdate}:{TdCp_List}")

        file.write(f"Refdate:{Refdate} Saved_Coupons Time:{Time_Now}\n")
        file.close()
