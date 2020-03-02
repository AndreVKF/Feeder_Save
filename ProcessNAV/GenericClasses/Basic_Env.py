from ProcessNAV.GenericClasses.API_BBG import *
from ProcessNAV.GenericClasses.SQL_Server_Connection import *

class Basic_Env():
    def __init__(self):
        super().__init__()

        self.RoundCash = 2
        self.RoundShareValue = 7   
        self.RoundShareAmount = 5
        self.RoundPrct = 4

        self.API_BBG = API_BBG()
        self.PM_Connection = SQL_Server_Connection(database='PM', verbose=False)
        # self.Bonds_DB_Connection = SQL_Server_Connection(database='Bonds_DB')
        # self.Pricing_Assets = SQL_Server_Connection(database='Pricing_Assets')

        