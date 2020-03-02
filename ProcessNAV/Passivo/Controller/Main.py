import sys
import datetime

from Passivo_Controller import Passivo_Controller
from ProcessNAV.GenericClasses.SQL_Server_Connection import *

if __name__=="__main__":
    # Refdate = 20200226
    Refdate = int(sys.argv[1])

    Passivo_Controller = Passivo_Controller(Refdate=Refdate)

    # Variaveis
    PM_Server_Connection = SQL_Server_Connection(database='PM')
    startTime = datetime.datetime.now()

    ################# Adjust Flow #################
    Passivo_Controller.Share_Operations()
    Passivo_Controller.Share_Subscriptions()
    Passivo_Controller.Share_Redemptions()
    Passivo_Controller.Shareholder_Position()
    Passivo_Controller.Passive_Funds()
    Passivo_Controller.Shareholders_Distribuidores_Rebate_Amount()

    PM_Server_Connection.execQuery(query=f"exec [dbo].[ProcessDBV3] '{int(Refdate)}'")
    print('Exec ProcessDBV3 finished !!')

    # Run Profit Table
    PM_Server_Connection.execQuery(query=f"exec [dbo].[SaveProfitTableT] '{int(Refdate)}'")
    print('Exec SaveProfitTableT finished !!')

    print(datetime.datetime.now()-startTime)
