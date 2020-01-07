from SQL_Server_Connector import SQL_Server_Connection
from Queries import Queries

from datetime import datetime
import sys

# Main Script
if __name__ == "__main__":
    # Variables and objects
    # refdate = 20191227
    refdate = int(sys.argv[1])
    srcFolder = "S:\\Management and Trading\\Risco & Compliance\\Comites\\Pos\\"
    srcSavePositionVaR = srcFolder + str(refdate) + ".csv"
    srcDailyVaRPosition = srcFolder + "TodayPosition.csv"


    Queries = Queries()
    SQL_Server_Connection = SQL_Server_Connection(database='PM')

    posVaRTable = SQL_Server_Connection.getData(query=Queries.getPositionVaR(refdate=refdate), dtparse=['RefDate'])
    posVaRTable.to_csv(srcSavePositionVaR, sep=";", index=False)

    dailyVaRPos = SQL_Server_Connection.getData(query=Queries.getPositionVaR(refdate= int(datetime.today().strftime("%Y%m%d"))), dtparse=['RefDate'])
    dailyVaRPos.to_csv(srcDailyVaRPosition, sep=";", index=False)



# dateList = [20191004,20191007,20191008,20191009,20191010,20191011,20191014,20191015,20191016,20191017,20191018,20191021,20191022,20191023,20191024,20191025,20191028,20191029,20191030,20191031,20191101,20191104,20191105,20191106,20191107,20191108,20191111,20191112,20191113,20191114,20191118,20191119,20191120,20191121,20191122,20191125,20191126,20191127,20191128,20191129,20191202,20191203,20191204,20191205,20191206,20191209,20191210,20191211,20191212,20191213,20191216,20191217,20191218,20191219,20191220,20191223,20191224,20191226]

# Queries = Queries()
# SQL_Server_Connection = SQL_Server_Connection(database='PM')

# for dt in dateList:
#     srcFolder = "S:\\Management and Trading\\Risco & Compliance\\Comites\\Pos\\" + str(dt) + ".csv"
#     posVaRTable = SQL_Server_Connection.getData(query=Queries.getPositionVaR(refdate=dt), dtparse=['RefDate'])

#     posVaRTable.to_csv(srcFolder, sep=";", index=False)