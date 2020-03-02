import sys
sys.path.insert(1, '..\\..\\GenericClasses\\')

import numpy as np
import pandas as pd
import datetime as dt

from datetime import datetime, timedelta
from bizdays import *

from Basic_Env import Basic_Env
from ProcessNAV.Passivo.Classes.PassivoQueries import *

class Views(Basic_Env):
    ################## CONSTRUCTOR ##################
    def __init__(self, Refdate):
        # super(Basic_Env, self).__init__()
        '''
        ########### Class Based DataFrames Return ###########
        Share_Operations
        Share_Subscriptions
        Share_Redemptions
        Shareholder_Position
        Passive_Funds
        #####################################################
        '''

        Basic_Env.__init__(self)
        self.Refdate = Refdate
        self.strRefdate = datetime(year=int(str(self.Refdate)[0:4]), month=int(str(self.Refdate)[4:6]), day=int(str(self.Refdate)[6:8])).strftime("%Y-%m-%d")
        self.dtRefdate = pd.to_datetime(str(self.Refdate), format="%Y%m%d")
        self.Queries = PassivoQueries(Refdate=Refdate)

        self.Errors_List = []

        '''
        ########### Col Names List ###########
        '''
        self.Share_Subscriptions_ColNames = [
            'Refdate'
            ,'Id_ShareMov'
            ,'Id_Shareholder'
            ,'Id_Entity'
            ,'Id_Product'
            ,'Id_PFeeType'
            ,'Id_RiskFactorBenchmark'
            ,'Td_Price'
            ,'Td_Value'
            ,'Quote_Date'
            ,'Open_ShareValue'
            ,'EffQuote_Date'
            ,'Subscription_Amount'
            ,'Subscription_Shares'
            ,'Td_OpenShares'
            ,'Td_RedempShares'
            ,'Td_CloseShares'
            ,'Td_CloseAmount'
            ,'PFee_DateBase'
            ,'PFee_ShareValue'
            ,'RiskFactor_Appreciation'
            ,'PFee_ShareBaseBenchmark'
            ,'PFeeBenchmark_Amount'
            ,'Td_Performance'
            ,'Closed']

        self.Shareholder_Position_ColNames = [
            'Refdate'
            ,'Id_Shareholder'
            ,'Shareholder'
            ,'Id_Product'
            ,'Product'
            ,'Open_ShareQtt'
            ,'Td_ShareMovQtt'
            ,'Td_SubsQtt'
            ,'Td_RedmQtt'
            ,'Td_CloseShareQtt'
            ,'Shareholder_MidShareValue'
            ,'Td_ShareValue'
            ,'Yd_ShareValue'
            ,'Shareholder_DailyPL'
            ,'Shareholder_TotalInvestment'
            ,'Shareholder_TotalPerformance'
            ,'Cont_Subscription'
            ,'Cont_Redemption'
            ,'Last_Update'
            ,'Id_Currency'
            ,'Id_Entity'
            ,'Inception_Date'
            ,'Total_Amount'
            ,'Since_Inception'
            ,'Since_InceptionXBenchmark'
            ,'MTD'
            ,'MTDXBenchmark'
            ,'YTD'
            ,'YTDXBenchmark']

        self.Passive_Funds_ColNames = [
            'Refdate',
            'Id_Entity',
            'Entity',
            'Id_Product',
            'Product',
            'Open_ShareQtt',
            'Td_ShareMovQtt',
            'Td_SubsQtt',
            'Td_RedmQtt',
            'Td_CloseShareQtt',
            'Td_ShareValue',
            'Yd_ShareValue',
            'Td_PL',
            'Total_Performance',
            'Entity_TotalApport',
            'Last_Update',
            'Id_Currency',
            'Td_NAV_Mov']

        self.Shareholders_Distribuidores_Rebate_Amount_ColNames = [
            'Refdate'
            ,'Id_ShareMov'
            ,'Shareholder'
            ,'Entity'
            ,'Distribuidor'
            ,'Td_Amount'
            ,'Tx_Adm'
            ,'Pct_Rebate'
            ,'Vl_Rebate']

        '''
        ########### Functions ###########
        '''
        # Create Calendar
        self.DF_Feriados_BRA = self.PM_Connection.getData(query = self.Queries.QFeriados_BRA())
        self.Create_Calendar()

        '''
        ########### Base DataFrames ###########
        '''
        self.DF_BaseShareMov = self.PM_Connection.getData(query = self.Queries.QShareMov_BaseQuery(), dtparse=['Request_Date', 'Quote_Date', 'EffQuote_Date', 'EffectiveFin_Date'])
        self.DF_Open_ShareSubscriptions = self.PM_Connection.getData(query = self.Queries.QOpenShareSubscriptions(), dtparse=['Refdate', 'Quote_Date', 'EffQuote_Date'])
        self.DF_Share_RedemptionsPriorRefdate = self.PM_Connection.getData(query = self.Queries.QShare_RedemptionsUpToRefdate(), dtparse=['Refdate'])
        self.DF_PFeeControl = self.PM_Connection.getData(query = self.Queries.QShare_PFeeControl(), dtparse=['Refdate', 'PFee_PaymentDate'])
        self.DF_RiskfactorIndexesValue = self.PM_Connection.getData(query = self.Queries.QIndexesValue_PFee(), dtparse=['Date'])
        self.DF_TdRedemptions = self.PM_Connection.getData(query = self.Queries.QTd_Redemptions(), dtparse=['Quote_Date', 'EffQuote_date', 'Request_Date'])
        self.DF_TdComeCotas = self.PM_Connection.getData(query = self.Queries.QComeCotas_DF(), dtparse=['Refdate', 'Quote_Date'])
        self.DF_Share_Operations_History = self.PM_Connection.getData(query = self.Queries.QShare_Operations_History_DF(), dtparse=['Refdate', 'EffQuote_date'])
        self.DF_Entities_ShareValue = self.PM_Connection.getData(query = self.Queries.QEntityShare_Value(), dtparse=['Refdate'])
        self.DF_Entities_ShareValue_History = self.PM_Connection.getData(query = self.Queries.QEntityShare_Value_History(), dtparse=['Refdate'])
        self.DF_Benchmark_Value_History = self.PM_Connection.getData(query = self.Queries.QBenchmark_Value(), dtparse=['Refdate'])
        self.DF_Entity_ProdData = self.PM_Connection.getData(query = self.Queries.QShare_ProdDescription())
        self.DF_Entities_AdmRebate = self.PM_Connection.getData(query = self.Queries.QEntities_AdmRebate())
        self.DF_Shareholders_AdmRebate = self.PM_Connection.getData(query = self.Queries.QShareholders_AdmRebate())
        '''
        ########### Upload DataFrames ###########
        '''
        self.DF_Share_Operations = self.Share_Operations()
        self.DF_Share_Subscriptions = self.Share_Subscriptions()
        # Create self.DF_Mod_Share_Subscriptions
        self.Update_ShareRedemptions()
        self.Adjust_Mod_Share_Subscriptions_Td_CloseShares()
        
        # Create Shareholder_Position
        self.DF_Shareholder_Position = self.Shareholder_Position()
        # Create Passive Funds
        self.DF_Passive_Funds = self.Passive_Funds()
        # Create Shareholders Distribuidores Rebate Amount
        self.DF_Shareholders_Distribuidores_Rebate_Amount = self.Update_Shareholders_Distribuidores_Rebate_Amount()

    '''
        ###################### CREATE DFs to Insert Into Database ######################
    '''
    def Share_Operations(self):
        """ Generates Share_Operations Table
            Update Share_Operations Table on Database
        """
        DF_Base = self.DF_BaseShareMov

        # Movimentação 1 => Cotas
        # Movimentação 2 => Financeiro
        # Movimentação 3 => Resgate Total

        # Determina Cash Amount de acordo com o tipe de movimentação
        DF_Base['Net_Amount'] = np.where(DF_Base['Id_Shareholder_MovType']==1, np.round(DF_Base['Value']*DF_Base['Amount'], self.RoundCash),
                                np.where(DF_Base['Id_Shareholder_MovType']==2, np.round(DF_Base['Amount'], self.RoundCash),
                                np.where(DF_Base['Id_Shareholder_MovType']==3, np.round(DF_Base['Yd_Shareholder_ShareQtt'].abs()*DF_Base['Value']*-1, self.RoundCash), np.NaN)))

        # Determina Number of Shares de acordo com o tipe de movimentação
        DF_Base['Number_Shares'] = np.where(DF_Base['Id_Shareholder_MovType']==1, np.round(DF_Base['Amount'], self.RoundShareValue),
                                np.where(DF_Base['Id_Shareholder_MovType']==2, np.round(DF_Base['Amount']/DF_Base['Value'], self.RoundShareValue),
                                np.where(DF_Base['Id_Shareholder_MovType']==3, np.round(DF_Base['Yd_Shareholder_ShareQtt'].abs()*-1, self.RoundShareValue), np.NaN)))

        # Share Operations DataFrame
        DF_Return = pd.DataFrame({
            'Id_ShareMov': DF_Base['Id_Share_Mov']
            ,'Shareholder_Id': DF_Base['Id_Shareholder']
            ,'Shareholder_Name': DF_Base['Shareholder']
            ,'Id_Entity': DF_Base['Id_Entity']
            ,'Entity_name': DF_Base['Entity']
            ,'Id_Product': DF_Base['Id_Product']
            ,'Product_name': DF_Base['Product']
            ,'Id_Currency': DF_Base['Id_Currency']
            ,'ShareValue': DF_Base['Value']
            ,'Request_Date': DF_Base['Request_Date']
            ,'Quote_Date': DF_Base['Quote_Date']
            ,'EffQuote_date': DF_Base['EffQuote_Date']
            ,'EffectiveFin_Date': DF_Base['EffectiveFin_Date']
            ,'Movimentação': DF_Base['OperationType']
            ,'Instrumento_Movimentação': DF_Base['MovType']
            ,'Net_Amount': DF_Base['Net_Amount']
            ,'Number_Shares': DF_Base['Number_Shares']
            ,'Close_Date': np.nan
            ,'Closed': np.nan
            ,'Distribuidor': DF_Base['Shareholders_Distribuidores_Name']
        })

        return DF_Return

    def Share_Subscriptions(self):
        """ Generates Daily Share_Subscriptions Base Table
            Base Table to Update as Share_Subscriptions on DB
        """
        # Gel all subscriptions positions without closed positions
        OpenShareDF_Base = self.DF_Open_ShareSubscriptions
        # Get Redemptions
        RedemptionsDF_Base = self.DF_Share_RedemptionsPriorRefdate
        RedemptionsDF_GroupedById_CloseShareMov = RedemptionsDF_Base[['Id_CloseShareMov', 'Closed_Shares']].groupby(['Id_CloseShareMov']).sum()
        # Get PFee Control
        PFeeDF = self.DF_PFeeControl
        PFeeDF_GroupedById_Last = PFeeDF.groupby(['Id_ShareMov']).nth(-1).reset_index()

        # Join with PFee Control
        OpenShareDF_Base = OpenShareDF_Base.merge(PFeeDF_GroupedById_Last, how='left', on='Id_ShareMov')

        # PFee Base Date / PFee Share Value
        OpenShareDF_Base['PFee_PaymentDate'] = np.where(OpenShareDF_Base['PFee_PaymentDate'].isnull(), OpenShareDF_Base['EffQuote_Date'], OpenShareDF_Base['PFee_PaymentDate'])
        OpenShareDF_Base['PFee_ShareValue'] = np.where(OpenShareDF_Base['New_PFeeShareValue'].isnull(), OpenShareDF_Base['Open_ShareValue'], OpenShareDF_Base['New_PFeeShareValue'])
        
        # Merge Open Positions with Redemptions
        OpenShareDF_Base = OpenShareDF_Base.merge(RedemptionsDF_GroupedById_CloseShareMov, how='left', left_on='Id_ShareMov', right_index=True)
        
        # Td_OpenShares
        '''
            Td Open Share Amount
            SUM from Share Operations where Refdate < ProcessDate
        '''
        OpenShareDF_Base['Td_OpenShares'] = OpenShareDF_Base['Number_Shares'] + OpenShareDF_Base['Closed_Shares'].fillna(0)
        
        ######### Base Calc PFee by Share_Subscription #########
        ######### PFee not Implemented Yet #########
        OpenShareDF_Base['RiskFactor_BaseDate'] = np.where(OpenShareDF_Base['PFee_PaymentDate'].isnull(),
            OpenShareDF_Base['EffQuote_Date'], 
            OpenShareDF_Base['PFee_PaymentDate'])
        
        OpenShareDF_Base['RiskFactor_EndDate'] = OpenShareDF_Base['Refdate']

        OpenShareDF_Base['RiskFactor_BaseValue'] = np.where(OpenShareDF_Base['New_PFeeShareValue'].isnull(),
            OpenShareDF_Base['Open_ShareValue'], 
            OpenShareDF_Base['New_PFeeShareValue'])

        # Merge Values to get Benchmark return
        OpenShareDF_Base = OpenShareDF_Base.merge(self.DF_RiskfactorIndexesValue, how='left', left_on=['Id_RiskFactorBenchmark', 'RiskFactor_BaseDate'], right_on=['Id_RiskFactor', 'Date'])
        OpenShareDF_Base.rename(columns={'Value': 'RiskFactor_BaseDate_Index'}, inplace=True)
        OpenShareDF_Base.drop(columns=['Date', 'Id_RiskFactor'], inplace=True)

        OpenShareDF_Base = OpenShareDF_Base.merge(self.DF_RiskfactorIndexesValue, how='left', left_on=['Id_RiskFactorBenchmark', 'RiskFactor_EndDate'], right_on=['Id_RiskFactor', 'Date'])
        OpenShareDF_Base.rename(columns={'Value': 'RiskFactor_EndDate_Index'}, inplace=True)
        OpenShareDF_Base.drop(columns=['Date', 'Id_RiskFactor'], inplace=True)

        # Benchmark PFee Value Accrued
        OpenShareDF_Base['IndexValue_Accrued'] = OpenShareDF_Base['RiskFactor_EndDate_Index'] / OpenShareDF_Base['RiskFactor_BaseDate_Index']
        # RiskFactor Benchmark Base Value with Accrual
        OpenShareDF_Base['IndexValue_OnBaseValue'] = OpenShareDF_Base['IndexValue_Accrued'] * OpenShareDF_Base['RiskFactor_BaseValue']

         # Share Subscriptions DataFrame
        DF_Return = pd.DataFrame({
            'Refdate': OpenShareDF_Base['Refdate']
            ,'Id_ShareMov': OpenShareDF_Base['Id_ShareMov']
            ,'Id_Shareholder': OpenShareDF_Base['Id_Shareholder']
            ,'Id_Entity': OpenShareDF_Base['Id_Entity']
            ,'Id_Product': OpenShareDF_Base['Id_Product']
            ,'Id_PFeeType': OpenShareDF_Base['Id_PFeeType']
            ,'Id_RiskFactorBenchmark': OpenShareDF_Base['Id_RiskFactorBenchmark']
            ,'Td_Price': np.round(OpenShareDF_Base['Td_Price'], self.RoundShareValue)
            ,'Td_Value': np.round(OpenShareDF_Base['Td_Value'], self.RoundShareValue)
            ,'Quote_Date': OpenShareDF_Base['Quote_Date']
            ,'Open_ShareValue': OpenShareDF_Base['Open_ShareValue']
            ,'EffQuote_Date': OpenShareDF_Base['EffQuote_Date']
            ,'Subscription_Amount': np.round(OpenShareDF_Base['Subscription_Amount'], self.RoundCash)
            ,'Subscription_Shares': np.round(OpenShareDF_Base['Number_Shares'], self.RoundShareAmount)
            ,'Td_OpenShares': np.round(OpenShareDF_Base['Td_OpenShares'], self.RoundShareValue)
            ,'Td_RedempShares': np.NaN
            ,'Td_CloseShares': np.round(OpenShareDF_Base['Td_OpenShares'], self.RoundShareValue)
            ,'Td_CloseAmount': np.NaN
            ,'PFee_DateBase': OpenShareDF_Base['PFee_PaymentDate']
            ,'PFee_ShareValue': np.round(OpenShareDF_Base['PFee_ShareValue'], self.RoundShareValue)
            ,'RiskFactor_Appreciation': np.round(OpenShareDF_Base['IndexValue_Accrued'], self.RoundShareValue)
            ,'PFee_ShareBaseBenchmark': np.round(OpenShareDF_Base['IndexValue_OnBaseValue'], self.RoundShareValue)
            ,'PFeeBenchmark_Amount': np.NaN
            ,'Td_Performance': np.NaN
            ,'Closed': np.NaN
        })

        return DF_Return

    def Shareholder_Position(self):
        """ Generates Daily Shareholder Position
            With Number of Subs/Redemp
            With Profit Table
        """
        dtRefdate = self.dtRefdate

        DF_All_Sh_Op = self.DF_Share_Operations_History[(self.DF_Share_Operations_History['EffQuote_date']<=dtRefdate)]
        DF_Hist_Sh_Op = self.DF_Share_Operations_History[(self.DF_Share_Operations_History['EffQuote_date']<dtRefdate)]
        DF_Td_Sh_Op = self.DF_Share_Operations_History[(self.DF_Share_Operations_History['EffQuote_date']==dtRefdate)]

        # ShareValue DF
        DF_ShareValue = self.DF_Entities_ShareValue

        colBase_List = ['Id_Shareholder'
            ,'Shareholder'
            ,'Id_Product'
            ,'Product']

        colBaseMov_List = ['Id_Shareholder'
            ,'Shareholder'
            ,'Id_Product'
            ,'Product'
            ,'Mov_Type']

        # Today Close Share Qtt
        DF_Base = DF_All_Sh_Op.groupby(colBase_List)['Number_Shares'].sum().round(5).reset_index()
        DF_Base = DF_Base[(DF_Base['Number_Shares'])>0].reset_index(drop=True)
        DF_Base.rename(columns={'Number_Shares': 'Td_CloseShareQtt'}, inplace=True)

        # Position D-1
        DF_Base_Hist = DF_Hist_Sh_Op.groupby(colBase_List)['Number_Shares'].sum().round(5).reset_index()
        DF_Base_Hist = DF_Base_Hist[(DF_Base_Hist['Number_Shares'])>0].reset_index(drop=True)
        DF_Base_Hist.rename(columns={'Number_Shares': 'Open_ShareQtt'}, inplace=True)

        # Join Position D0 with D-1
        DF_Base = DF_Base.merge(DF_Base_Hist, how='left', on=colBase_List)

        ############ Daily Share Mov ############
        # Total Daily Share_Mov
        DF_Base['Td_ShareMovQtt'] = np.round(DF_Base['Td_CloseShareQtt'] - DF_Base['Open_ShareQtt'].fillna(0), 5)
        DF_Base['Td_ShareMovQtt'] = np.where(DF_Base['Td_ShareMovQtt']==0, np.nan, DF_Base['Td_ShareMovQtt'])

        # Daily Subscriptions
        DF_Td_Subscriptions = DF_Td_Sh_Op[(DF_Td_Sh_Op['Mov_Type']=='Aplicação') 
            & (DF_Td_Sh_Op['Number_Shares']>0)].groupby(colBase_List)['Number_Shares'].sum().round(5).reset_index()
        DF_Base = DF_Base.merge(DF_Td_Subscriptions, how='left', on=colBase_List)
        DF_Base.rename(columns={'Number_Shares': 'Td_SubsQtt'}, inplace=True)

        # Daily Redemptions
        DF_Td_Redemptions = DF_Td_Sh_Op[(DF_Td_Sh_Op['Mov_Type']!='Aplicação') 
            & (DF_Td_Sh_Op['Number_Shares']<0)].groupby(colBase_List)['Number_Shares'].sum().round(5).reset_index()
        DF_Base = DF_Base.merge(DF_Td_Redemptions, how='left', on=colBase_List)
        DF_Base.rename(columns={'Number_Shares': 'Td_RedmQtt'}, inplace=True)


        ############ Count Share_Mov ############
        # Subscriptions
        DF_ShareholderSubs = DF_All_Sh_Op[colBaseMov_List]
        DF_ShareholderSubs = DF_ShareholderSubs[(DF_ShareholderSubs['Mov_Type']=='Aplicação')].groupby(colBase_List)['Mov_Type'].count().reset_index()
        DF_ShareholderSubs.rename(columns={'Mov_Type': 'Cont_Subscription'}, inplace=True)
        DF_Base = DF_Base.merge(DF_ShareholderSubs, how='left', on=colBase_List)

        # Redemptions
        DF_ShareholderRedemp = DF_All_Sh_Op[colBaseMov_List]
        DF_ShareholderRedemp = DF_ShareholderRedemp[(DF_ShareholderRedemp['Mov_Type']=='Resgate') | (DF_ShareholderRedemp['Mov_Type']=='Resgate Total')].groupby(colBase_List)['Mov_Type'].count().reset_index()
        DF_ShareholderRedemp.rename(columns={'Mov_Type': 'Cont_Redemption'}, inplace=True)
        DF_Base = DF_Base.merge(DF_ShareholderRedemp, how='left', on=colBase_List)
                
        ########### Base DFs  ###########
        # Base Shareholder Position
        DF_Shareholder_Position = DF_Base
        # Share Value History
        DF_Entities_ShareValue_History = self.DF_Entities_ShareValue_History[['Refdate', 'Id_Product', 'Value']]
        # Benchmark Value History
        DF_Benchmark_Value_History = self.DF_Benchmark_Value_History[['Refdate', 'Id_RiskFactorBenchmark', 'Value']]
        # Date List
        DF_Dates = pd.DataFrame(data=DF_Entities_ShareValue_History['Refdate'].unique(), columns=['Refdate']).sort_values(by='Refdate', ascending=False)

        # Base for Shareholder Profit Table
        colBaseList_DF_Base_Shareholder_PT = ['Refdate'
            ,'Id_ShareMov'
            ,'Id_Shareholder'
            ,'Id_Entity'
            ,'Id_Product'
            ,'Id_RiskFactorBenchmark'
            ,'EffQuote_Date'
            ,'Open_ShareValue'
            ,'Subscription_Amount'
            ,'Td_OpenShares'
            ,'Td_CloseShares']

        groupBy_ColList = ['Id_Shareholder'
            ,'Id_Product']


        ########### Id_Currency / Id_Entity ###########
        DF_Shareholder_Position = DF_Shareholder_Position.merge(self.DF_Entity_ProdData, how='left', on='Id_Product')

        ########### Base DF Subscription Table D-1 (Open Positions) ###########
        DF_Base_Shareholder_PT = self.DF_Mod_Share_Subscriptions[(self.DF_Mod_Share_Subscriptions['EffQuote_Date']<self.dtRefdate)][colBaseList_DF_Base_Shareholder_PT]

        ########### Base DF Subscription Table D-0 (Total Subscribers Open Positions) ###########
        DF_Base_Shareholder_PT_AllSubs = self.DF_Mod_Share_Subscriptions[colBaseList_DF_Base_Shareholder_PT]

        '''
            Share_Subscription Data
        '''

        ########### Inception Date ###########
        DF_Shareholder_Inception = DF_Base_Shareholder_PT_AllSubs.loc[DF_Base_Shareholder_PT_AllSubs.groupby(groupBy_ColList)['EffQuote_Date'].idxmin()][['Id_Shareholder', 'Id_Product', 'EffQuote_Date']]
        DF_Shareholder_Inception.rename(columns={"EffQuote_Date": 'Inception_Date'}, inplace=True)
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Shareholder_Inception, how='left', on=groupBy_ColList)

        ########### Shareholder MidShareValue ###########
        DF_Shareholder_MidShareValue = DF_Base_Shareholder_PT.groupby(groupBy_ColList).apply(lambda x: pd.Series([sum(x.Td_OpenShares*x.Open_ShareValue)/sum(x.Td_OpenShares)]))
        DF_Shareholder_MidShareValue.reset_index(inplace=True)
        DF_Shareholder_MidShareValue.rename(columns={0: 'Shareholder_MidShareValue'}, inplace=True)
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Shareholder_MidShareValue, how='left', on=groupBy_ColList)

        ########### Td_ShareValue/Yd_ShareValue ###########
        DF_Shareholder_Position['Refdate'] = DF_Dates.iloc[0].to_dict()['Refdate']
        DF_Shareholder_Position['YdDate'] = DF_Dates.iloc[1].to_dict()['Refdate']

        # Td_ShareValue
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Entities_ShareValue_History.rename(columns={'Refdate': 'Refdate' ,'Value': 'Td_ShareValue'}), how='left', left_on=['Refdate', 'Id_Product'], right_on=['Refdate', 'Id_Product'])

        # Yd_ShareValue
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Entities_ShareValue_History.rename(columns={'Refdate': 'YdDate', 'Value': 'Yd_ShareValue'}), how='left', left_on=['YdDate', 'Id_Product'], right_on=['YdDate', 'Id_Product'])

        # Shareholder Daily PL
        DF_Shareholder_Position['Shareholder_DailyPL'] = DF_Shareholder_Position['Open_ShareQtt'] * (DF_Shareholder_Position['Td_ShareValue'] - DF_Shareholder_Position['Yd_ShareValue'])

        ########### Total Amount Opening ###########
        DF_Shareholder_Position['Total_Amount_Opening'] = (DF_Shareholder_Position['Open_ShareQtt'] * DF_Shareholder_Position['Td_ShareValue']).round(2)

        ########### Total Amount ###########
        DF_Shareholder_Position['Total_Amount'] = (DF_Shareholder_Position['Td_CloseShareQtt'] * DF_Shareholder_Position['Td_ShareValue']).round(2)

        ########### Shareholders Total Investment ###########
        # Total Investment Including Refdate Subscriptions
        DF_Shareholder_TotalInvestment = DF_Base_Shareholder_PT_AllSubs.groupby(groupBy_ColList).apply(lambda x: pd.Series([sum(x.Td_OpenShares*x.Open_ShareValue)]).round(2))
        DF_Shareholder_TotalInvestment.reset_index(inplace=True)
        DF_Shareholder_TotalInvestment.rename(columns={0: 'Shareholder_TotalInvestment'}, inplace=True)
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Shareholder_TotalInvestment, how='left', on=groupBy_ColList)

        # Total Investment Excluding Refdate Subscriptions
        DF_Shareholder_TotalInvestment = DF_Base_Shareholder_PT.groupby(groupBy_ColList).apply(lambda x: pd.Series([sum(x.Td_OpenShares*x.Open_ShareValue)]).round(2))
        DF_Shareholder_TotalInvestment.reset_index(inplace=True)
        DF_Shareholder_TotalInvestment.rename(columns={0: 'Shareholder_TotalInvestment_Profit'}, inplace=True)
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Shareholder_TotalInvestment, how='left', on=groupBy_ColList)

        '''
            Profitability Table Info
        '''
        ########### Since Inception Profitability ###########
        # Since Inception Profitability
        DF_Shareholder_Position['Since_Inception'] = (DF_Shareholder_Position['Total_Amount_Opening'] / (DF_Shareholder_Position['Shareholder_TotalInvestment_Profit']) - 1).round(4)

        ########### Since Inception x Benchmark ###########
        # Total Bizdays
        DF_Shareholder_Position["Since_Inception_BizDays"] = DF_Shareholder_Position.apply(lambda x: self.cal.bizdays(x.Inception_Date.strftime('%Y-%m-%d'), x.Refdate.strftime('%Y-%m-%d')), axis=1)
        # Benchmark Value at Refdate
        DF_Base_Shareholder_PT = DF_Base_Shareholder_PT.merge(DF_Benchmark_Value_History.rename(columns={'Value': 'Benchmark_at_Refdate'}), how='left', left_on=['Refdate', 'Id_RiskFactorBenchmark'], right_on=['Refdate', 'Id_RiskFactorBenchmark'])
        # Benchmark Value at Inception
        DF_Base_Shareholder_PT = DF_Base_Shareholder_PT.merge(DF_Benchmark_Value_History.rename(columns={'Refdate': 'EffQuote_Date','Value': 'Benchmark_at_Inception'}), how='left', left_on=['EffQuote_Date', 'Id_RiskFactorBenchmark'], right_on=['EffQuote_Date', 'Id_RiskFactorBenchmark'])
        DF_Base_Shareholder_PT['Since_Inception_Benchmark_Carry'] = (DF_Base_Shareholder_PT['Benchmark_at_Refdate'] / DF_Base_Shareholder_PT['Benchmark_at_Inception']) * (DF_Base_Shareholder_PT['Td_OpenShares'] * DF_Base_Shareholder_PT['Open_ShareValue']).round(2)

        DF_Shareholder_Total_Since_Inception_Benchmark_Carry = DF_Base_Shareholder_PT.groupby(groupBy_ColList).apply(lambda x: pd.Series([sum(x.Since_Inception_Benchmark_Carry)]))
        DF_Shareholder_Total_Since_Inception_Benchmark_Carry.reset_index(inplace=True)
        DF_Shareholder_Total_Since_Inception_Benchmark_Carry.rename(columns={0: 'Total_Since_Inception_Benchmark_Carry'}, inplace=True)
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Shareholder_Total_Since_Inception_Benchmark_Carry, how='left', on=groupBy_ColList)

        # Since Inception Benchmark Profitability
        DF_Shareholder_Position['Since_Inception_Benchmark'] = DF_Shareholder_Position['Total_Since_Inception_Benchmark_Carry'] / DF_Shareholder_Position['Shareholder_TotalInvestment_Profit'] - 1

        # Since Inception Profit. x Benchmark Profit.
        DF_Shareholder_Position['Since_InceptionXBenchmark'] = ((DF_Shareholder_Position['Since_Inception'] + 1) ** (1 / DF_Shareholder_Position['Since_Inception_BizDays']) - 1) \
            / ((DF_Shareholder_Position['Since_Inception_Benchmark'] + 1) ** (1 / DF_Shareholder_Position['Since_Inception_BizDays']) - 1)

        ########### MTD Profitability ###########
        # IniDate Month Base Date
        '''
            If EffQuote_Date >= First Date of Month:
                EffQuote_Date
            Else:
                Last Date of Previous Month
        '''
        DF_Base_Shareholder_PT['Month_Ini_Date'] = dtRefdate.replace(day=1)
        DF_Base_Shareholder_PT['Month_Base_Date'] = np.where(DF_Base_Shareholder_PT['EffQuote_Date']>=DF_Base_Shareholder_PT['Month_Ini_Date']
            ,DF_Base_Shareholder_PT['EffQuote_Date']
            ,DF_Dates[(DF_Dates['Refdate']<dtRefdate.replace(day=1))].sort_values('Refdate', ascending=False).iloc[0])

        ########### MTD ###########
        # ShareValue at Month_Base_Date
        DF_Base_Shareholder_PT = DF_Base_Shareholder_PT.merge(DF_Entities_ShareValue_History.rename(columns={'Refdate': 'Month_Base_Date', 'Value': 'Share_Value_Month_Base_Date'}), how='left', on=['Id_Product', 'Month_Base_Date'])

        # Month Base Date Value
        DF_Shareholder_MonthBase_TotalInvestment = DF_Base_Shareholder_PT.groupby(groupBy_ColList).apply(lambda x: pd.Series([sum(x.Share_Value_Month_Base_Date*x.Td_OpenShares)]).round(2))
        DF_Shareholder_MonthBase_TotalInvestment.reset_index(inplace=True)
        DF_Shareholder_MonthBase_TotalInvestment.rename(columns={0: 'Shareholder_Month_Base_TotalInvestment'}, inplace=True)
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Shareholder_MonthBase_TotalInvestment, how='left', on=groupBy_ColList)

        # MTD Profitability
        DF_Shareholder_Position['MTD'] = (DF_Shareholder_Position['Total_Amount_Opening'] / (DF_Shareholder_Position['Shareholder_Month_Base_TotalInvestment']) - 1).round(4)

        ########### MTD x Benchmark ###########
        # Benchmark at Month_Base_Date 
        DF_Base_Shareholder_PT = DF_Base_Shareholder_PT.merge(DF_Benchmark_Value_History.rename(columns={'Refdate': 'Month_Base_Date', 'Value': 'Benchmark_Value_Month_Base_Date'}), how='left', on=['Id_RiskFactorBenchmark', 'Month_Base_Date'])

        DF_Benchmark_MonthBase_TotalInvestment = DF_Base_Shareholder_PT.groupby(groupBy_ColList).apply(lambda x: pd.Series([sum(x.Share_Value_Month_Base_Date * x.Td_OpenShares * x.Benchmark_at_Refdate / x.Benchmark_Value_Month_Base_Date)]).round(2))
        DF_Benchmark_MonthBase_TotalInvestment.reset_index(inplace=True)
        DF_Benchmark_MonthBase_TotalInvestment.rename(columns={0: 'Benchmark_Month_Base_TotalInvestment'}, inplace=True)
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Benchmark_MonthBase_TotalInvestment, how='left', on=groupBy_ColList)

        DF_Shareholder_Position['Benchmark_Month_Profit'] = DF_Shareholder_Position['Benchmark_Month_Base_TotalInvestment'] / DF_Shareholder_Position['Shareholder_Month_Base_TotalInvestment'] - 1

        DF_Shareholder_Position['Month_Base_Date'] = np.where(DF_Shareholder_Position['Inception_Date']>=dtRefdate.replace(day=1)
            ,DF_Shareholder_Position['Inception_Date']
            ,DF_Dates[(DF_Dates['Refdate']<dtRefdate.replace(day=1))].sort_values('Refdate', ascending=False).iloc[0])

        # Since Inception Profit. x Benchmark Profit.
        # Month to Date Total Bizdays
        DF_Shareholder_Position["MTD_BizDays"] = DF_Shareholder_Position.apply(lambda x: self.cal.bizdays(x.Month_Base_Date.strftime('%Y-%m-%d'), x.Refdate.strftime('%Y-%m-%d')), axis=1)

        DF_Shareholder_Position['MTDXBenchmark'] = ((DF_Shareholder_Position['MTD'] + 1) ** (1 / DF_Shareholder_Position['MTD_BizDays']) - 1) \
            / ((DF_Shareholder_Position['Benchmark_Month_Profit'] + 1) ** (1 / DF_Shareholder_Position['MTD_BizDays']) - 1)

        ########### YTD Profitability ###########
        # IniDate Year Base Date
        '''
            If EffQuote_Date >= First Date of Year:
                EffQuote_Date
            Else:
                Last Date of Previous Year
        '''
        DF_Base_Shareholder_PT['Year_Ini_Date'] = dtRefdate.replace(day=1, month=1)
        DF_Base_Shareholder_PT['Year_Base_Date'] = np.where(DF_Base_Shareholder_PT['EffQuote_Date']>=DF_Base_Shareholder_PT['Year_Ini_Date']
            ,DF_Base_Shareholder_PT['EffQuote_Date']
            ,DF_Dates[(DF_Dates['Refdate']<dtRefdate.replace(day=1, month=1))].sort_values('Refdate', ascending=False).iloc[0])


        ########### YTD ###########
        # ShareValue at Year_Base_Date
        DF_Base_Shareholder_PT = DF_Base_Shareholder_PT.merge(DF_Entities_ShareValue_History.rename(columns={'Refdate': 'Year_Base_Date', 'Value': 'Share_Value_Year_Base_Date'}), how='left', on=['Id_Product', 'Year_Base_Date'])

        # Year Base Date Value
        DF_Shareholder_YearBase_TotalInvestment = DF_Base_Shareholder_PT.groupby(groupBy_ColList).apply(lambda x: pd.Series([sum(x.Share_Value_Year_Base_Date*x.Td_OpenShares)]).round(2))
        DF_Shareholder_YearBase_TotalInvestment.reset_index(inplace=True)
        DF_Shareholder_YearBase_TotalInvestment.rename(columns={0: 'Shareholder_Year_Base_TotalInvestment'}, inplace=True)
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Shareholder_YearBase_TotalInvestment, how='left', on=groupBy_ColList)

        # YTD Profitability
        DF_Shareholder_Position['YTD'] = (DF_Shareholder_Position['Total_Amount_Opening'] / (DF_Shareholder_Position['Shareholder_Year_Base_TotalInvestment']) - 1).round(4)

        ########### YTD x Benchmark ###########
        # Benchmark at Year_Base_Date 
        DF_Base_Shareholder_PT = DF_Base_Shareholder_PT.merge(DF_Benchmark_Value_History.rename(columns={'Refdate': 'Year_Base_Date', 'Value': 'Benchmark_Value_Year_Base_Date'}), how='left', on=['Id_RiskFactorBenchmark', 'Year_Base_Date'])

        DF_Benchmark_YearBase_TotalInvestment = DF_Base_Shareholder_PT.groupby(groupBy_ColList).apply(lambda x: pd.Series([sum(x.Share_Value_Year_Base_Date * x.Td_OpenShares * x.Benchmark_at_Refdate / x.Benchmark_Value_Year_Base_Date)]).round(2))
        DF_Benchmark_YearBase_TotalInvestment.reset_index(inplace=True)
        DF_Benchmark_YearBase_TotalInvestment.rename(columns={0: 'Benchmark_Year_Base_TotalInvestment'}, inplace=True)
        DF_Shareholder_Position = DF_Shareholder_Position.merge(DF_Benchmark_YearBase_TotalInvestment, how='left', on=groupBy_ColList)

        DF_Shareholder_Position['Benchmark_Year_Profit'] = DF_Shareholder_Position['Benchmark_Year_Base_TotalInvestment'] / DF_Shareholder_Position['Shareholder_Year_Base_TotalInvestment'] - 1

        DF_Shareholder_Position['Year_Base_Date'] = np.where(DF_Shareholder_Position['Inception_Date']>=dtRefdate.replace(day=1)
            ,DF_Shareholder_Position['Inception_Date']
            ,DF_Dates[(DF_Dates['Refdate']<dtRefdate.replace(day=1, month=1))].sort_values('Refdate', ascending=False).iloc[0])

        # Since Inception Profit. x Benchmark Profit.
        # Year to Date Total Bizdays
        DF_Shareholder_Position["YTD_BizDays"] = DF_Shareholder_Position.apply(lambda x: self.cal.bizdays(x.Year_Base_Date.strftime('%Y-%m-%d'), x.Refdate.strftime('%Y-%m-%d')), axis=1)

        DF_Shareholder_Position['YTDXBenchmark'] = ((DF_Shareholder_Position['YTD'] + 1) ** (1 / DF_Shareholder_Position['YTD_BizDays']) - 1) \
            / ((DF_Shareholder_Position['Benchmark_Year_Profit'] + 1) ** (1 / DF_Shareholder_Position['YTD_BizDays']) - 1)

        # Shareholder TotalPerformance
        DF_Shareholder_Position['Shareholder_TotalPerformance'] = np.nan

        # Shareholder Last_Update
        DF_Shareholder_Position['Last_Update'] = pd.to_datetime('today')

        '''
            Data frame to check values
        '''
        self.Full_Check_Shareholder_Position = DF_Shareholder_Position

        DF_Return = pd.DataFrame({
            'Refdate': DF_Shareholder_Position['Refdate']
            ,'Id_Shareholder': DF_Shareholder_Position['Id_Shareholder']
            ,'Shareholder': DF_Shareholder_Position['Shareholder']
            ,'Id_Product': DF_Shareholder_Position['Id_Product']
            ,'Product': DF_Shareholder_Position['Product']
            ,'Open_ShareQtt': np.round(DF_Shareholder_Position['Open_ShareQtt'], self.RoundShareAmount)
            ,'Td_ShareMovQtt': np.round(DF_Shareholder_Position['Td_ShareMovQtt'], self.RoundShareAmount)
            ,'Td_SubsQtt': np.round(DF_Shareholder_Position['Td_SubsQtt'], self.RoundShareAmount)
            ,'Td_RedmQtt': np.round(DF_Shareholder_Position['Td_RedmQtt'], self.RoundShareAmount)
            ,'Td_CloseShareQtt': np.round(DF_Shareholder_Position['Td_CloseShareQtt'], self.RoundShareAmount)
            ,'Shareholder_MidShareValue': np.round(DF_Shareholder_Position['Shareholder_MidShareValue'], self.RoundShareValue)
            ,'Td_ShareValue': np.round(DF_Shareholder_Position['Td_ShareValue'], self.RoundShareValue)
            ,'Yd_ShareValue': np.round(DF_Shareholder_Position['Yd_ShareValue'], self.RoundShareValue)
            ,'Shareholder_DailyPL': np.round(DF_Shareholder_Position['Shareholder_DailyPL'], self.RoundCash)
            ,'Shareholder_TotalInvestment': np.round(DF_Shareholder_Position['Shareholder_TotalInvestment'], self.RoundCash)
            ,'Shareholder_TotalPerformance': np.round(DF_Shareholder_Position['Shareholder_TotalPerformance'], self.RoundCash)
            ,'Cont_Subscription': np.round(DF_Shareholder_Position['Cont_Subscription'], 0)
            ,'Cont_Redemption': np.round(DF_Shareholder_Position['Cont_Redemption'], 0)
            ,'Last_Update': DF_Shareholder_Position['Last_Update']
            ,'Id_Currency': DF_Shareholder_Position['Id_Currency']
            ,'Id_Entity': DF_Shareholder_Position['Id_Entity']
            ,'Inception_Date': DF_Shareholder_Position['Inception_Date']
            ,'Total_Amount': np.round(DF_Shareholder_Position['Total_Amount'], self.RoundCash)
            ,'Since_Inception': np.round(DF_Shareholder_Position['Since_Inception'], self.RoundPrct)
            ,'Since_InceptionXBenchmark': np.round(DF_Shareholder_Position['Since_InceptionXBenchmark'], self.RoundPrct)
            ,'MTD': np.round(DF_Shareholder_Position['MTD'], self.RoundPrct)
            ,'MTDXBenchmark': np.round(DF_Shareholder_Position['MTDXBenchmark'], self.RoundPrct)
            ,'YTD': np.round(DF_Shareholder_Position['YTD'], self.RoundPrct)
            ,'YTDXBenchmark': np.round(DF_Shareholder_Position['YTDXBenchmark'], self.RoundPrct)
        })

        return DF_Return

    def Passive_Funds(self):
        """
            Function to create Passive Funds Table
            Update to Database
        """
        # Group by Entities
        groubBy_List = ['Id_Product'
            ,'Product']

        # Share_Subscriptions Table
        DF_Entity_TotalApport = self.DF_Mod_Share_Subscriptions.groupby(['Id_Entity', 'Id_Product']).apply(lambda x: pd.Series(sum(x.Td_OpenShares * x.Open_ShareValue)).round(2))
        DF_Entity_TotalApport.reset_index(inplace=True)
        DF_Entity_TotalApport.rename(columns={0: 'Entity_TotalApport'}, inplace=True)

        # Open Entities Share Value DataFrame
        DF_ShareValue = self.DF_Entities_ShareValue

        # Share Position DataFrames
        DF_All_Sh_Op = self.DF_Share_Operations_History[(self.DF_Share_Operations_History['EffQuote_date']<=self.dtRefdate)]
        DF_Hist_Sh_Op = self.DF_Share_Operations_History[(self.DF_Share_Operations_History['EffQuote_date']<self.dtRefdate)]
        DF_Td_Sh_Op = self.DF_Share_Operations_History[(self.DF_Share_Operations_History['EffQuote_date']==self.dtRefdate)]

        ###############  Base DataFrame ###############
        # Based on Td_CloseShareQtt
        DF_Base = DF_All_Sh_Op.rename(columns={'Number_Shares': 'Td_CloseShareQtt'}).groupby(groubBy_List)['Td_CloseShareQtt'].sum().round(5).reset_index()
        # Merge with Open Share Position
        DF_Base = DF_Base.merge(DF_Hist_Sh_Op.rename(columns={'Number_Shares': 'Open_ShareQtt'}).groupby(groubBy_List)['Open_ShareQtt'].sum().round(5).reset_index(), how='left', on=['Id_Product', 'Product'])

        ############### Add Today Moviments ###############
        # Adds Today Subscriptions
        DF_Base = DF_Base.merge(DF_Td_Sh_Op[(DF_Td_Sh_Op['Mov_Type']=='Aplicação')].rename(columns={'Number_Shares': 'Td_SubsQtt'}).groupby(groubBy_List)['Td_SubsQtt'].sum().round(5).reset_index(), how='left', on=['Id_Product', 'Product'])
        # Adds Today Redemptions
        DF_Base = DF_Base.merge(DF_Td_Sh_Op[(DF_Td_Sh_Op['Mov_Type']!='Aplicação')].rename(columns={'Number_Shares': 'Td_RedmQtt'}).groupby(groubBy_List)['Td_RedmQtt'].sum().round(5).reset_index(), how='left', on=['Id_Product', 'Product'])
        # Adds Today Total Moviments
        DF_Base = DF_Base.merge(DF_Td_Sh_Op.rename(columns={'Number_Shares': 'Td_ShareMovQtt'}).groupby(groubBy_List)['Td_ShareMovQtt'].sum().round(5).reset_index(), how='left', on=['Id_Product', 'Product'])

        ############### Adds Share Value ###############
        DF_Base = DF_Base.merge(DF_ShareValue[['Entity' ,'Id_Product', 'Id_Entity', 'Id_Currency', 'TdValue', 'YdValue']], how='left', on='Id_Product')

        ############### Calc Columns ###############
        # Td_PL
        DF_Base['Td_PL'] = DF_Base['Open_ShareQtt'] * (DF_Base['TdValue'] - DF_Base['YdValue'])
        # Entity Total Apport
        DF_Base = DF_Base.merge(DF_Entity_TotalApport, how='left', on=['Id_Entity', 'Id_Product'])
        # Last Update
        DF_Base['Last_Update'] = pd.to_datetime('today')
        # Td_NAV Mov
        DF_Base['Td_NAV_Mov'] = (DF_Base['Td_ShareMovQtt'] * DF_Base['TdValue']).round(2)

        ############### Rename Some Columns / Return ###############
        DF_Base['Total_Performance'] = np.nan
        DF_Base['Refdate'] = self.dtRefdate
        DF_Base.rename(columns={'TdValue': 'Td_ShareValue', 'YdValue': 'Yd_ShareValue'}, inplace=True)

        DF_Return = pd.DataFrame({
            'Refdate': DF_Base['Refdate']
            ,'Id_Entity': DF_Base['Id_Entity']
            ,'Entity': DF_Base['Entity']
            ,'Id_Product': DF_Base['Id_Product']
            ,'Product': DF_Base['Product']
            ,'Open_ShareQtt': np.round(DF_Base['Open_ShareQtt'], self.RoundShareAmount)
            ,'Td_ShareMovQtt': np.round(DF_Base['Td_ShareMovQtt'], self.RoundShareAmount)
            ,'Td_SubsQtt': np.round(DF_Base['Td_SubsQtt'], self.RoundShareAmount)
            ,'Td_RedmQtt': np.round(DF_Base['Td_RedmQtt'], self.RoundShareAmount)
            ,'Td_CloseShareQtt': np.round(DF_Base['Td_CloseShareQtt'], self.RoundShareAmount)
            ,'Td_ShareValue': np.round(DF_Base['Td_ShareValue'], self.RoundShareValue)
            ,'Yd_ShareValue': np.round(DF_Base['Yd_ShareValue'], self.RoundShareValue)
            ,'Td_PL': np.round(DF_Base['Td_PL'], self.RoundCash)
            ,'Total_Performance': np.round(DF_Base['Total_Performance'], self.RoundCash)
            ,'Entity_TotalApport': np.round(DF_Base['Entity_TotalApport'], self.RoundCash)
            ,'Last_Update': DF_Base['Last_Update']
            ,'Id_Currency': DF_Base['Id_Currency']
            ,'Td_NAV_Mov': np.round(DF_Base['Td_NAV_Mov'], self.RoundCash)
        })

        return DF_Return


    def Update_Shareholders_Distribuidores_Rebate_Amount(self):
        """ Function to update the amount rebate shareholders por distribuidor
            Updates to Database
        """

        Base_DF = self.DF_Mod_Share_Subscriptions[['Refdate'
            ,'Id_ShareMov'
            ,'Id_Shareholder'
            ,'Id_Entity'
            ,'Td_CloseAmount']]
        Entities_DF = self.DF_Entities_AdmRebate
        Shareholders_DF = self.DF_Shareholders_AdmRebate

        # Base DF
        Base_DF = Base_DF.merge(Entities_DF, how='left', on='Id_Entity')
        Base_DF = Base_DF.merge(Shareholders_DF, how='left', on='Id_Shareholder')

        Base_DF['Vl_Rebate'] = np.where(Base_DF['Id_Shareholders_Distribuidores_Rebate']==1,
            Base_DF['Td_CloseAmount'] * ((1 + Base_DF['Value']) ** (1 / 252) - 1) * Base_DF['Rebate_Amount'],
                np.where(Base_DF['Id_Shareholders_Distribuidores_Rebate']==2,
                Base_DF['Td_CloseAmount'] * ((1 + Base_DF['Rebate_Amount']) ** (1 / 252) - 1),
                np.nan))

        Base_DF.rename(columns={'Rebate_Amount': 'Pct_Rebate',
            'Value': 'Tx_Adm',
            'Td_CloseAmount' :'Td_Amount'
            ,'Shareholder_Distribuidor': 'Distribuidor'}, inplace=True)

        return Base_DF[self.Shareholders_Distribuidores_Rebate_Amount_ColNames]
        
    '''
    ###################### MAIN FUNCTIONS ######################
    '''    
    def Update_ShareRedemptions(self):
        """ Function to update shareholders Redemptions
            Adjust Share_Subscriptions with Daily Redemptions
            Update: Shares_Subscriptions Table with Daily Redemptions
            Insert: Inserts Redemptions/Come Cotas on Share_Redemptions Table
        """

        self.DF_Mod_Share_Subscriptions = self.DF_Share_Subscriptions
        createShare_Redemptions_DF = False

        ####### Adjustment for Share Redemptions #######
        ############## REDEMPTIONS ##############
        Redemption_DF = self.DF_TdRedemptions[(self.DF_TdRedemptions['Movimentação']=='Resgate Total') 
            | (self.DF_TdRedemptions['Movimentação']=='Resgate')]

        # Empty DataFrame
        self.DF_Share_Redemptions = pd.DataFrame(columns=['Refdate'
            ,'Id_ShareMov'
            ,'Id_CloseShareMov'
            ,'Id_Shareholder'
            ,'Id_Product'
            ,'Request_Date'
            ,'Td_Value'
            ,'Closed_Amount'
            ,'Closed_Shares'])

        ####### Redemptions Adjustment #######
        if not Redemption_DF.empty:
            Redemption_DF.apply(self.Adjust_Share_Subscriptions, axis=1)

            # Create Share_Redemption Dataframe
            self.DF_Share_Redemptions = pd.DataFrame(self.Share_Redemptions_List,
                columns=['Refdate'
                ,'Id_ShareMov'
                ,'Id_CloseShareMov'
                ,'Id_Shareholder'
                ,'Id_Product'
                ,'Request_Date'
                ,'Td_Value'
                ,'Closed_Amount'
                ,'Closed_Shares'])

        ############## COME COTAS ##############
        if not self.DF_TdComeCotas.empty:
            # Adjust Share_Subscriptions
            ComeCotas_AggDF = self.DF_TdComeCotas.groupby(['Id_CloseShareMov'])['Closed_Shares'].sum().reset_index()
            ComeCotas_AggDF['Id_CloseShareMov'] = pd.to_numeric(ComeCotas_AggDF['Id_CloseShareMov'])
            ComeCotas_AggDF.rename(columns={'Closed_Shares': "ComeCotas_ShareQtt"}, inplace=True)

            self.Adjust_Share_Subscriptions_ComeCotas(ComeCotas_AggDF)
            # Append Come Cotas Data Frame
            try:
                self.DF_Share_Redemptions = pd.concat([self.DF_Share_Redemptions, self.DF_TdComeCotas])
            except:
                self.DF_Share_Redemptions = self.DF_TdComeCotas

        ############## Adjust Columns ##############
        if not Redemption_DF.empty or not self.DF_TdComeCotas.empty:
            self.DF_Share_Redemptions = pd.DataFrame({
                'Refdate': self.DF_Share_Redemptions['Refdate']
                ,'Id_ShareMov': self.DF_Share_Redemptions['Id_ShareMov']
                ,'Id_CloseShareMov': self.DF_Share_Redemptions['Id_CloseShareMov']
                ,'Id_Shareholder': self.DF_Share_Redemptions['Id_Shareholder']
                ,'Id_Product': self.DF_Share_Redemptions['Id_Product']
                ,'Request_Date': self.DF_Share_Redemptions['Request_Date']
                ,'Td_Value': np.round(self.DF_Share_Redemptions['Td_Value'], self.RoundShareValue)
                ,'Closed_Amount': np.round(self.DF_Share_Redemptions['Closed_Amount'], self.RoundCash)
                ,'Closed_Shares': np.round(self.DF_Share_Redemptions['Closed_Shares'], self.RoundShareAmount)
            })


    def Adjust_Mod_Share_Subscriptions_Td_CloseShares(self):
        """Function to adjust Share_Subscriptions columns
        [Td_CloseShares]
        """
        self.DF_Mod_Share_Subscriptions['Td_CloseShares'] = np.where(self.DF_Mod_Share_Subscriptions['Closed']!=1, np.round(self.DF_Mod_Share_Subscriptions['Td_OpenShares'] + np.nan_to_num(self.DF_Mod_Share_Subscriptions['Td_RedempShares']), self.RoundShareAmount), 0)
        self.DF_Mod_Share_Subscriptions['Td_CloseAmount'] = np.where(self.DF_Mod_Share_Subscriptions['Closed']!=1, np.round(self.DF_Mod_Share_Subscriptions['Td_CloseShares'] * self.DF_Mod_Share_Subscriptions['Td_Value'], self.RoundCash), 0)

    '''
    ###################### AUXILIAR FUNCTIONS ######################
    '''    

    def Create_Calendar(self):
        """Function to create self.calendar base
        """
        self.DF_Feriados_BRA['Data'].to_csv('Holidays.csv', header=False, index=None)
        self.holidays = load_holidays('Holidays.csv')

        self.cal = Calendar(self.holidays, ['Sunday', 'Saturday'], name='Brazil')

    def Adjust_Share_Subscriptions_ComeCotas(self, ComeCotas_AggDF):
        """Function to adjust Share_Subscriptions position
        Adds Share Moviment due from ComeCotas
        
        Arguments:
            ComeCotas_AggDF {pd.DataFrame} -- DataFrame with ComeCotas position to be deducted]
        """
        self.DF_Mod_Share_Subscriptions = self.DF_Mod_Share_Subscriptions.merge(ComeCotas_AggDF, how='left', left_on='Id_ShareMov', right_on='Id_CloseShareMov')
        # Adjust Come Cotas on Share_Subscriptions
        # Tratamento dos cases where Total Redemptions has effective quoted on the same day
        self.DF_Mod_Share_Subscriptions['Td_RedempShares'] = np.where((self.DF_Mod_Share_Subscriptions['Closed']!=1) & (self.DF_Mod_Share_Subscriptions['Id_CloseShareMov'].notna()), np.nan_to_num(self.DF_Mod_Share_Subscriptions['Td_RedempShares']) + np.nan_to_num(self.DF_Mod_Share_Subscriptions['ComeCotas_ShareQtt']), self.DF_Mod_Share_Subscriptions['Td_RedempShares'])

        # Return DF_Mod_Share_Subscriptions Base Columns
        self.DF_Mod_Share_Subscriptions =  self.DF_Mod_Share_Subscriptions[self.Share_Subscriptions_ColNames]

    def Adjust_Share_Subscriptions(self, row):
        """Adjust Share_Subscriptions with Daily Redemptions
        Apply function to a Dataframe
        
        Arguments:
            row {pd.DataFrame.row} -- Share_Operation Redemptions Row 
        
        Returns:
            In Case of Redemption amount being higher then total subscriptions: Return None
        """
        # Local Variables
        Id_Shareholder = row['Shareholder_Id']
        Id_Product = row['Id_Product']
        
        # Base for creating Share_Redemptions
        self.Share_Redemptions_List = []

        if row['Movimentação']=='Resgate':
            Redemp = row['Number_Shares']

            # Check if redemption amount is less than or equal to shareholder total amount invested
            if abs(Redemp) > self.DF_Mod_Share_Subscriptions[(self.DF_Mod_Share_Subscriptions['Id_Shareholder']==Id_Shareholder) 
                & (self.DF_Mod_Share_Subscriptions['Id_Product']==Id_Product) 
                & (self.DF_Mod_Share_Subscriptions['Closed']!=1)]['Td_OpenShares'].sum():
                self.Errors_List.append(f"Error in updating Share Subscriptions: Share_Mov={row['Id_ShareMov'].value}")

                return None

            # Update DF_Mod_Share_Subscriptions to adjust for daily Redemptions
            for index, rowSubs in self.DF_Mod_Share_Subscriptions[(self.DF_Mod_Share_Subscriptions['Id_Shareholder']==Id_Shareholder) 
                & (self.DF_Mod_Share_Subscriptions['Id_Product']==Id_Product) 
                & (self.DF_Mod_Share_Subscriptions['Closed']!=1)].sort_values(by=['EffQuote_Date']).iterrows():
                ##### Case to check how many subscriptions should be closed #####
                # Case if Redemption Share Amount is equal to Subscription Amount
                # Exit Loop
                if np.round(abs(Redemp), self.RoundShareAmount)==np.round(rowSubs['Td_CloseShares'], self.RoundShareAmount):
                    self.DF_Mod_Share_Subscriptions.loc[index, 'Td_RedempShares'] = np.round(np.nan_to_num(self.DF_Mod_Share_Subscriptions.loc[index]['Td_OpenShares'])*-1, self.RoundShareAmount)
                    self.DF_Mod_Share_Subscriptions.loc[index, 'Td_CloseShares'] = 0
                    self.DF_Mod_Share_Subscriptions.loc[index, 'Closed'] = 1

                    # Fill Share_Redemptions_List
                    self.Share_Redemptions_List.append((self.strRefdate
                        ,row['Id_ShareMov']
                        ,rowSubs['Id_ShareMov']
                        ,Id_Shareholder
                        ,rowSubs['Id_Product']
                        ,row['Request_Date']
                        ,row['ShareValue']
                        ,abs(row['ShareValue']*Redemp)*-1
                        ,Redemp))
                    break

                # Case if Redemption Share Amount exceeds the Subscription Share Amount
                # Go To next Subscription Share Amount
                elif np.round(abs(Redemp), self.RoundShareAmount)>np.round(rowSubs['Td_CloseShares'], self.RoundShareAmount):
                    self.DF_Mod_Share_Subscriptions.loc[index, 'Td_RedempShares'] = np.round(np.nan_to_num(self.DF_Mod_Share_Subscriptions.loc[index]['Td_OpenShares'])*-1, self.RoundShareAmount)
                    self.DF_Mod_Share_Subscriptions.loc[index, 'Td_CloseShares'] = 0
                    self.DF_Mod_Share_Subscriptions.loc[index, 'Closed'] = 1

                    # Fill Share_Redemptions_List
                    self.Share_Redemptions_List.append((self.strRefdate
                        ,row['Id_ShareMov']
                        ,rowSubs['Id_ShareMov']
                        ,Id_Shareholder
                        ,rowSubs['Id_Product']
                        ,row['Request_Date']
                        ,row['ShareValue']
                        ,abs(row['ShareValue']*row['Td_CloseShares'])*-1
                        ,row['Td_CloseShares']))

                    # Update Redemp
                    Redemp+=np.round(row['Td_CloseShares'], self.RoundShareAmount)

                # Case if Redemption Share Amount is less than the Subscription Share Amount
                # Partial close of Subscription Share Amount
                # Exit Loop
                elif np.round(abs(Redemp), self.RoundShareAmount)<np.round(rowSubs['Td_CloseShares'], self.RoundShareAmount):
                    self.DF_Mod_Share_Subscriptions.loc[index, 'Td_RedempShares'] = np.round(np.nan_to_num(self.DF_Mod_Share_Subscriptions.loc[index]['Td_RedempShares']) + abs(Redemp)*-1, self.RoundShareAmount)
                    # self.DF_Mod_Share_Subscriptions.loc[index, 'Td_CloseShares'] = np.round(self.DF_Mod_Share_Subscriptions.loc[index]['Td_CloseShares'] + self.DF_Mod_Share_Subscriptions.loc[index]['Td_RedempShares'], self.RoundShareAmount)

                    # Fill Share_Redemptions_List
                    self.Share_Redemptions_List.append((self.strRefdate
                        ,row['Id_ShareMov']
                        ,rowSubs['Id_ShareMov']
                        ,Id_Shareholder
                        ,rowSubs['Id_Product']
                        ,row['Request_Date']
                        ,row['ShareValue']
                        ,abs(row['ShareValue']*Redemp)*-1
                        ,Redemp))
                    break
        
        elif row['Movimentação']=='Resgate Total':
            
            for index, rowSubs in self.DF_Mod_Share_Subscriptions[(self.DF_Mod_Share_Subscriptions['Id_Shareholder']==Id_Shareholder) 
                & (self.DF_Mod_Share_Subscriptions['Id_Product']==Id_Product) 
                & (self.DF_Mod_Share_Subscriptions['Closed']!=1)].sort_values(by=['EffQuote_Date']).iterrows():
                ##### Case to check how many subscriptions should be closed #####
                # Case if Redemption Share Amount is equal to Subscription Amount
                # Exit Loop
                self.DF_Mod_Share_Subscriptions.loc[index, 'Td_RedempShares'] = np.round(np.nan_to_num(self.DF_Mod_Share_Subscriptions.loc[index]['Td_OpenShares']*-1) , self.RoundShareAmount)
                self.DF_Mod_Share_Subscriptions.loc[index, 'Td_CloseShares'] = 0
                self.DF_Mod_Share_Subscriptions.loc[index, 'Closed'] = 1

                # Fill Share_Redemptions_List
                self.Share_Redemptions_List.append((self.strRefdate
                    ,row['Id_ShareMov']
                    ,rowSubs['Id_ShareMov']
                    ,Id_Shareholder
                    ,rowSubs['Id_Product']
                    ,row['Request_Date']
                    ,row['ShareValue']
                    ,abs(row['ShareValue']*Redemp)*-1
                    ,Redemp))
       
