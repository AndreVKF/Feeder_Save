from SQL_Server_Connector import SQL_Server_Connection
from Queries import Queries
from Functions import *


# main
if __name__ == "__main__":
    # Time now
    Time_Now = datetime.now().strftime("%Y%m%d %H:%M:%S")

    # Set Refdate as Integer
    # Refdate = 20190424
    Refdate = int(sys.argv[1])
    Log_DirSrc = sys.argv[2]

    # Classes
    Queries = Queries()
    SQL_Server_Connection = SQL_Server_Connection(database='PM')

    # Exec Process NAV
    Fun_Exec = Exec_PositionBondsData(Refdate=Refdate, Log_DirSrc=Log_DirSrc,
                                      Queries=Queries, SQL_Server_Connection=SQL_Server_Connection)
