import threading
import time
import datetime
import runpy
import subprocess
import sys

# Timer Class


class TimerClass(threading.Thread):
    def __init__(self, Refdate, Interval, Log_DirSrc, Activate):
        threading.Thread.__init__(self)
        self.event = threading.Event()
        self.Refdate = Refdate
        self.Interval = Interval
        self.Log_DirSrc = Log_DirSrc
        self.Activate = Activate

    def run(self):
        # Aciona Timer
        if self.Activate:
            while not self.event.is_set():
                sys.argv = ['', str(self.Refdate), self.Log_DirSrc]
                runpy.run_path('./Save_Prices.py', run_name='__main__')
                runpy.run_path('./Check_Prices_Repeat.py', run_name='__main__')
                runpy.run_path('./Check_Prices_Over.py', run_name='__main__')
                runpy.run_path('./Exec_ProcessNAV.py', run_name='__main__')
                runpy.run_path('./Exec_Save_PositionBonds.py', run_name='__main__')
                runpy.run_path('./Save_Position_VaR.py', run_name='__main__')
                self.event.wait(60 * self.Interval)
                # print(str(datetime.datetime.now()))
                # self.event.wait(15)
        else:
            sys.argv = ['', str(self.Refdate), self.Log_DirSrc]
            runpy.run_path('./Save_Prices.py', run_name='__main__')
            runpy.run_path('./Check_Prices_Repeat.py', run_name='__main__')
            runpy.run_path('./Check_Prices_Over.py', run_name='__main__')
            runpy.run_path('./Exec_ProcessNAV.py', run_name='__main__')
            runpy.run_path('./Exec_Save_PositionBonds.py', run_name='__main__')
            runpy.run_path('./Save_Position_VaR.py', run_name='__main__')
            self.event.set()

    def stop(self):
        self.event.set()


# tmr = TimerClass(20190422, 2)
# tmr.start()
# tmr.stop()
