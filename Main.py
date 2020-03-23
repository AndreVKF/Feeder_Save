import Save_Coupons
import Save_Indexes
from Timer import TimerClass

import runpy
import sys
import time
from datetime import datetime

from ProcessNAV.GenericClasses.API_BBG import API_BBG

sys.path.insert(1, 'S:\\Management and Trading\\Feeder_Prices\\Python\\ProcessNAV\\GenericClasses')
sys.path.insert(1, 'S:\\Management and Trading\\Feeder_Prices\\Python\\ProcessNAV\\Passivo\\Classes')
sys.path.insert(1, 'S:\\Management and Trading\\Feeder_Prices\\Python\\ProcessNAV\\Passivo\\Controller')

# Main Script
if __name__ == '__main__':

    # Variaveis cmd locais
    Refdate = input("Refdate<aaaammdd>: ")
    Activate_Timer = input("Activate Timer<y/n>: ")
    time.sleep(2)

    # Try input
    while Activate_Timer != 'y' and Activate_Timer != 'n':
        Activate_Timer = input("Activate Timer<y/n>: ")
        time.sleep(2)

    if Activate_Timer == 'y':
        Get_Px_Interval = input("Get Prices Interval<minutes>: ")
        Get_Px_Interval = int(Get_Px_Interval)
        Activate = True
    elif Activate_Timer == 'n':
        Get_Px_Interval = 0
        Activate = False

    # Refdate = 20190429
    # Refdate = datetime.now().strftime("%Y%m%d")
    # Get_Px_Interval = 15
    # Activate = False

    # Create Log File
    Log_DirSrc = "S:\\Management and Trading\\Feeder_Prices\\logs\\" + \
        datetime.now().strftime("%Y%m%d_%H%M%S") + ".txt"
    file = open(Log_DirSrc, "w+")
    file.close()

    # Save Indexes / Coupons
    sys.argv = ['', str(Refdate), Log_DirSrc]
    runpy.run_path('./Save_Indexes.py', run_name='__main__')
    runpy.run_path('./Save_Coupons.py', run_name='__main__')

    # Initiate Price Timer
    Timer = TimerClass(Refdate=Refdate, Interval=Get_Px_Interval,
                       Log_DirSrc=Log_DirSrc, Activate=Activate)
    Timer.start()

    # Timer.stop()