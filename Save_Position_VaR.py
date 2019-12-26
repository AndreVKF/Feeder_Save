from SQL_Server_Connector import SQL_Server_Connection
from Queries import Queries

from datetime import datetime
import sys

# Main Script
if __name__ == "__main__":
    # Variables and objects
    # refdate = 20191226
    refdate = int(sys.argv[1])
    srcFolder = "S:\\Management and Trading\\Risco & Compliance\\Comites\\Pos\\" + str(refdate) + ".csv"

    Queries = Queries()
    SQL_Server_Connection = SQL_Server_Connection(database='PM')

    posVaRTable = SQL_Server_Connection.getData(query=Queries.getPositionVaR(refdate=refdate), dtparse=['RefDate'])

    posVaRTable.to_csv(srcFolder, sep=";", index=False)