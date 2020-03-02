import sys
import runpy
import datetime

sys.path.insert(1, 'GenericClasses\\')
sys.path.insert(1, 'Passivo\\Classes\\')
sys.path.insert(1, 'Passivo\\Controller\\')

from SQL_Server_Connector import SQL_Server_Connection

# Main Script
if __name__ == '__main__':
    # Variaveis
    PM_Server_Connection = SQL_Server_Connection(database='PM')
    startTime = datetime.datetime.now()

    # Run Passivo
    Refdate = int(sys.argv[1])
    runpy.run_path('./ProcessNAV/Passivo/Controller/Main.py', run_name='__main__')

    # Run Ativo
    PM_Server_Connection.execQuery(query=f"exec [dbo].[ProcessDBV3] '{int(Refdate)}'")
    print('Exec ProcessDBV3 finished !!')

    # Run Profit Table
    PM_Server_Connection.execQuery(query=f"exec [dbo].[SaveProfitTableT] '{int(Refdate)}'")
    print('Exec SaveProfitTableT finished !!')

    print(datetime.datetime.now()-startTime)

