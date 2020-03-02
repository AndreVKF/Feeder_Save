import sys
sys.path.insert(1, '..\\..\\GenericClasses\\')
sys.path.insert(1, '..\\..\\Passivo\\Classes\\')

from Basic_Env import Basic_Env
from ProcessNAV.Passivo.Classes.PassivoQueries import *
from Views import Views


'''
    Flow Controller
'''

class Passivo_Controller(Basic_Env):
    def __init__(self, Refdate):
        Basic_Env.__init__(self)

        ########################### Arguments ###########################
        self.Refdate = Refdate
        self.Queries = PassivoQueries(Refdate=Refdate)
        self.Views = Views(Refdate=Refdate)
        self.SQL_Server_Connection = self.PM_Connection
        self.erroList = []

    ########################### Flow Controller ###########################

    def Share_Operations(self):
        '''Update Daily Share_Operations
            Create Daily Share_Operations Table based on Daily Share_Mov Table
            Update on Database
        '''

        # # Create Base Dataframe
        # Share_Operations = self.Views.DF_Share_Operations
        # print('1')

        # # Delete History
        # self.SQL_Server_Connection.execQuery(query=self.Queries.DelShare_Operations())
        # print('2')

        # # Insert New Data Frame
        # if not Share_Operations.empty:
        #     self.SQL_Server_Connection.insertDataFrame(tableDB='Share_Operations', df=Share_Operations)
        #     print('3')

        try:
            # Create Base Dataframe
            Share_Operations = self.Views.DF_Share_Operations

            # Delete History
            self.SQL_Server_Connection.execQuery(query=self.Queries.DelShare_Operations())

            # Insert New Data Frame
            if not Share_Operations.empty:
                self.SQL_Server_Connection.insertDataFrame(tableDB='Share_Operations', df=Share_Operations)

        except:
            self.erroList.append('Error on updating Share_Operations table.')


    def Share_Subscriptions(self):
        '''Update Daily Share_Subscriptions
            Create Daily Share_Subscriptions Table
            Update on Database
        '''

        try:
            # Create Base Dataframe
            Share_Subscriptions = self.Views.DF_Mod_Share_Subscriptions

            # Delete History
            self.SQL_Server_Connection.execQuery(query=self.Queries.DelShare_Subscriptions())

            # Insert New Data Frame
            if not Share_Subscriptions.empty:
                self.SQL_Server_Connection.insertDataFrame(tableDB='Share_Subscriptions', df=Share_Subscriptions)

        except:
            self.erroList.append('Error on updating Share_Subscriptions table.')

    def Share_Redemptions(self):
        '''Update Daily Share_Redemptions
            Create Daily Share_Redemptions Table
            Update on Database
        '''

        try:
            # Create Base Dataframe
            Share_Redemptions = self.Views.DF_Share_Redemptions

            # Delete History
            self.SQL_Server_Connection.execQuery(query=self.Queries.DelShare_Redemptions())

            # Insert New Data Frame
            if not Share_Redemptions.empty:
                self.SQL_Server_Connection.insertDataFrame(tableDB='Share_Redemptions', df=Share_Redemptions)

        except:
            self.erroList.append('Error on updating Share_Redemptions table.')

    def Shareholder_Position(self):
        '''Update Daily Shareholder_Position
            Create Daily Shareholder_Position Table
            Update on Database
        '''

        try:
            # Create Base Dataframe
            Shareholder_Position = self.Views.DF_Shareholder_Position

            # Delete History
            self.SQL_Server_Connection.execQuery(query=self.Queries.DelShareholder_Position())

            # Insert New Data Frame
            if not Shareholder_Position.empty:
                self.SQL_Server_Connection.insertDataFrame(tableDB='Shareholder_Position', df=Shareholder_Position)

        except:
            self.erroList.append('Error on updating Shareholder_Position table.')

    def Passive_Funds(self):
        '''Update Daily Passive_Funds
            Create Daily Passive_Funds Table
            Update on Database
        '''

        try:
            # Create Base Dataframe
            Passive_Funds = self.Views.DF_Passive_Funds

            # Delete History
            self.SQL_Server_Connection.execQuery(query=self.Queries.DelPassive_Funds())

            # Insert New Data Frame
            if not Passive_Funds.empty:
                self.SQL_Server_Connection.insertDataFrame(tableDB='Passive_Funds', df=Passive_Funds)

        except:
            self.erroList.append('Error on updating Passive_Funds table.')

    def Shareholders_Distribuidores_Rebate_Amount(self):
        '''Update Daily Shareholders Distribuidores
            Create Daily Shareholders Distribuidores Table
            Update on Database
        '''

        try:
            # Create Base Dataframe
            Shareholders_Distribuidores_Rebate_Amount = self.Views.DF_Shareholders_Distribuidores_Rebate_Amount

            # Delete History
            self.SQL_Server_Connection.execQuery(query=self.Queries.DelShareholders_Distribuidores_Rebate_Amount())

            # Insert New Data Frame
            if not Shareholders_Distribuidores_Rebate_Amount.empty:
                self.SQL_Server_Connection.insertDataFrame(tableDB='Shareholders_Distribuidores_Rebate_Amount', df=Shareholders_Distribuidores_Rebate_Amount)

        except:
            self.erroList.append('Error on updating Shareholders_Distribuidores_Rebate_Amount table.')

